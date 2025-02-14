/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.TextNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


/**
 * A base class for a round-trip worker which will produce and consume equal number of messages.
 *
 * This is used to create a round-trip trogdor agent which will spawn producers and consumers to
 * produce and consume equal number of messages based on the workload it is executing.
 *
 * Currently, there are 2 subclasses, one which uses {@link org.apache.kafka.clients.consumer.KafkaConsumer}
 * and another which uses {@link org.apache.kafka.clients.consumer.KafkaShareConsumer} as the consumer.
 */
public abstract class RoundTripWorkerBase implements TaskWorker {
    private static final int THROTTLE_PERIOD_MS = 100;

    private static final int LOG_INTERVAL_MS = 5000;

    private static final int LOG_NUM_MESSAGES = 10;

    private static final Logger log = LoggerFactory.getLogger(RoundTripWorkerBase.class);

    private static final PayloadGenerator KEY_GENERATOR = new SequentialPayloadGenerator(4, 0);

    private ToReceiveTracker toReceiveTracker;

    protected String id;

    protected RoundTripWorkloadSpec spec;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final Lock lock = new ReentrantLock();

    private final Condition unackedSendsAreZero = lock.newCondition();

    private ScheduledExecutorService executor;

    private WorkerStatusTracker status;

    private KafkaFutureImpl<String> doneFuture;

    private KafkaProducer<byte[], byte[]> producer;

    private Long unackedSends;

    private ToSendTracker toSendTracker;

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("RoundTripWorker is already running.");
        }
        log.info("{}: Activating RoundTripWorker.", id);
        this.executor = Executors.newScheduledThreadPool(3,
            ThreadUtils.createThreadFactory("RoundTripWorker%d", false));
        this.status = status;
        this.doneFuture = doneFuture;
        this.producer = null;
        this.unackedSends = spec.maxMessages();
        executor.submit(new Prepare());
    }

    class Prepare implements Runnable {
        @Override
        public void run() {
            try {
                if (spec.targetMessagesPerSec() <= 0) {
                    throw new ConfigException("Can't have targetMessagesPerSec <= 0.");
                }
                Map<String, NewTopic> newTopics = new HashMap<>();
                HashSet<TopicPartition> active = new HashSet<>();
                for (Map.Entry<String, PartitionsSpec> entry :
                    spec.activeTopics().materialize().entrySet()) {
                    String topicName = entry.getKey();
                    PartitionsSpec partSpec = entry.getValue();
                    newTopics.put(topicName, partSpec.newTopic(topicName));
                    for (Integer partitionNumber : partSpec.partitionNumbers()) {
                        active.add(new TopicPartition(topicName, partitionNumber));
                    }
                }
                if (active.isEmpty()) {
                    throw new RuntimeException("You must specify at least one active topic.");
                }
                status.update(new TextNode("Creating " + newTopics.keySet().size() + " topic(s)"));
                WorkerUtils.createTopics(log, spec.bootstrapServers(), spec.commonClientConf(),
                    spec.adminClientConf(), newTopics, false);
                status.update(new TextNode("Created " + newTopics.keySet().size() + " topic(s)"));
                toSendTracker = new ToSendTracker(spec.maxMessages());
                toReceiveTracker = new ToReceiveTracker();
                executor.submit(new ProducerRunnable(active));
                executor.submit(new ConsumerRunnable(active));
                executor.submit(new StatusUpdater());
                executor.scheduleWithFixedDelay(
                    new StatusUpdater(), 30, 30, TimeUnit.SECONDS);
            } catch (Throwable e) {
                WorkerUtils.abort(log, "Prepare", e, doneFuture);
            }
        }
    }

    private static class ToSendTrackerResult {
        final long index;
        final boolean firstSend;

        ToSendTrackerResult(long index, boolean firstSend) {
            this.index = index;
            this.firstSend = firstSend;
        }
    }

    private static class ToSendTracker {
        private final long maxMessages;
        private final List<Long> failed = new ArrayList<>();
        private long frontier = 0;

        ToSendTracker(long maxMessages) {
            this.maxMessages = maxMessages;
        }

        synchronized void addFailed(long index) {
            failed.add(index);
        }

        synchronized long frontier() {
            return frontier;
        }

        synchronized ToSendTrackerResult next() {
            if (failed.isEmpty()) {
                if (frontier >= maxMessages) {
                    return null;
                } else {
                    return new ToSendTrackerResult(frontier++, true);
                }
            } else {
                return new ToSendTrackerResult(failed.remove(0), false);
            }
        }
    }

    class ProducerRunnable implements Runnable {
        private final HashSet<TopicPartition> partitions;
        private final Throttle throttle;

        ProducerRunnable(HashSet<TopicPartition> partitions) {
            this.partitions = partitions;
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 4 * 16 * 1024L);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000L);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer." + id);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 105000);
            // user may over-write the defaults with common client config and producer config
            WorkerUtils.addConfigsToProperties(props, spec.commonClientConf(), spec.producerConf());
            producer = new KafkaProducer<>(props, new ByteArraySerializer(),
                new ByteArraySerializer());
            int perPeriod = WorkerUtils.
                perSecToPerPeriod(spec.targetMessagesPerSec(), THROTTLE_PERIOD_MS);
            this.throttle = new Throttle(perPeriod, THROTTLE_PERIOD_MS);
        }

        @Override
        public void run() {
            long messagesSent = 0;
            long uniqueMessagesSent = 0;
            log.debug("{}: Starting RoundTripWorker#ProducerRunnable.", id);
            try {
                Iterator<TopicPartition> iter = partitions.iterator();
                while (true) {
                    final ToSendTrackerResult result = toSendTracker.next();
                    if (result == null) {
                        break;
                    }
                    throttle.increment();
                    final long messageIndex = result.index;
                    if (result.firstSend) {
                        toReceiveTracker.addPending(messageIndex);
                        uniqueMessagesSent++;
                    }
                    messagesSent++;
                    if (!iter.hasNext()) {
                        iter = partitions.iterator();
                    }
                    TopicPartition partition = iter.next();
                    // we explicitly specify generator position based on message index
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(partition.topic(),
                        partition.partition(), KEY_GENERATOR.generate(messageIndex),
                        spec.valueGenerator().generate(messageIndex));
                    producer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            lock.lock();
                            try {
                                unackedSends -= 1;
                                if (unackedSends <= 0)
                                    unackedSendsAreZero.signalAll();
                            } finally {
                                lock.unlock();
                            }
                        } else {
                            log.info("{}: Got exception when sending message {}: {}",
                                id, messageIndex, exception.getMessage());
                            toSendTracker.addFailed(messageIndex);
                        }
                    });
                }
            } catch (Throwable e) {
                WorkerUtils.abort(log, "ProducerRunnable", e, doneFuture);
            } finally {
                lock.lock();
                try {
                    log.info("{}: ProducerRunnable is exiting.  messagesSent={}; uniqueMessagesSent={}; " +
                                    "ackedSends={}/{}.", id, messagesSent, uniqueMessagesSent,
                            spec.maxMessages() - unackedSends, spec.maxMessages());
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private class ToReceiveTracker {
        private final TreeSet<Long> pending = new TreeSet<>();

        private long totalReceived = 0;

        synchronized void addPending(long messageIndex) {
            pending.add(messageIndex);
        }

        synchronized boolean removePending(long messageIndex) {
            if (pending.remove(messageIndex)) {
                totalReceived++;
                return true;
            } else {
                return false;
            }
        }

        synchronized long totalReceived() {
            return totalReceived;
        }

        void log() {
            long numToReceive;
            List<Long> list = new ArrayList<>(LOG_NUM_MESSAGES);
            synchronized (this) {
                numToReceive = pending.size();
                for (Iterator<Long> iter = pending.iterator();
                        iter.hasNext() && (list.size() < LOG_NUM_MESSAGES); ) {
                    Long i = iter.next();
                    list.add(i);
                }
            }
            log.info("{}: consumer waiting for {} message(s), starting with: {}",
                id, numToReceive, list.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }
    }

    class ConsumerRunnable implements Runnable {

        ConsumerRunnable(HashSet<TopicPartition> partitions) {
            initializeConsumer(partitions);
        }

        @Override
        public void run() {
            long uniqueMessagesReceived = 0;
            long messagesReceived = 0;
            long pollInvoked = 0;
            log.debug("{}: Starting RoundTripWorker#ConsumerRunnable.", id);
            try {
                long lastLogTimeMs = Time.SYSTEM.milliseconds();
                while (true) {
                    try {
                        pollInvoked++;
                        ConsumerRecords<byte[], byte[]> records = fetchRecords(Duration.ofMillis(50));
                        for (ConsumerRecord<byte[], byte[]> record : records) {
                            int messageIndex = ByteBuffer.wrap(record.key()).order(ByteOrder.LITTLE_ENDIAN).getInt();
                            messagesReceived++;
                            if (toReceiveTracker.removePending(messageIndex)) {
                                uniqueMessagesReceived++;
                                if (uniqueMessagesReceived >= spec.maxMessages()) {
                                    lock.lock();
                                    try {
                                        log.info("{}: Consumer received the full count of {} unique messages.  " +
                                                "Waiting for all {} sends to be acked...", id, spec.maxMessages(), unackedSends);
                                        while (unackedSends > 0)
                                            unackedSendsAreZero.await();
                                    } finally {
                                        lock.unlock();
                                    }

                                    log.info("{}: all sends have been acked.", id);
                                    new StatusUpdater().update();
                                    doneFuture.complete("");
                                    return;
                                }
                            }
                        }
                        long curTimeMs = Time.SYSTEM.milliseconds();
                        if (curTimeMs > lastLogTimeMs + LOG_INTERVAL_MS) {
                            toReceiveTracker.log();
                            lastLogTimeMs = curTimeMs;
                        }
                    } catch (WakeupException e) {
                        log.debug("{}: Consumer got WakeupException", id, e);
                    } catch (TimeoutException e) {
                        log.debug("{}: Consumer got TimeoutException", id, e);
                    }
                }
            } catch (Throwable e) {
                WorkerUtils.abort(log, "ConsumerRunnable", e, doneFuture);
            } finally {
                log.info("{}: ConsumerRunnable is exiting.  Invoked poll {} time(s).  " +
                    "messagesReceived = {}; uniqueMessagesReceived = {}.",
                    id, pollInvoked, messagesReceived, uniqueMessagesReceived);
            }
        }
    }

    public class StatusUpdater implements Runnable {
        @Override
        public void run() {
            try {
                update();
            } catch (Exception e) {
                WorkerUtils.abort(log, "StatusUpdater", e, doneFuture);
            }
        }

        StatusData update() {
            StatusData statusData =
                new StatusData(toSendTracker.frontier(), toReceiveTracker.totalReceived());
            status.update(JsonUtil.JSON_SERDE.valueToTree(statusData));
            return statusData;
        }
    }

    public static class StatusData {
        private final long totalUniqueSent;
        private final long totalReceived;

        @JsonCreator
        public StatusData(@JsonProperty("totalUniqueSent") long totalUniqueSent,
                          @JsonProperty("totalReceived") long totalReceived) {
            this.totalUniqueSent = totalUniqueSent;
            this.totalReceived = totalReceived;
        }

        @JsonProperty
        public long totalUniqueSent() {
            return totalUniqueSent;
        }

        @JsonProperty
        public long totalReceived() {
            return totalReceived;
        }
    }

    /**
     * Initialize the consumer.
     */
    protected abstract void initializeConsumer(HashSet<TopicPartition> partitions);

    /**
     *
     * Invoke poll from the consumer and return the records fetched.
     */
    protected abstract ConsumerRecords<byte[], byte[]> fetchRecords(Duration duration);

    /**
     * Close the consumer.
     */
    protected abstract void shutdownConsumer();

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("RoundTripWorker is not running.");
        }
        log.info("{}: Deactivating RoundTripWorker.", id);
        doneFuture.complete("");
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.DAYS);
        shutdownConsumer();
        Utils.closeQuietly(producer, "producer");
        this.producer = null;
        this.unackedSends = null;
        this.executor = null;
        this.doneFuture = null;
        log.info("{}: Deactivated RoundTripWorker.", id);
    }
}
