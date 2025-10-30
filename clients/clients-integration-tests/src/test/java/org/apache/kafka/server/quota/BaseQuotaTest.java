package org.apache.kafka.server.quota;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import java.util.concurrent.Future;

@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = "log.unclean.leader.election.enable", value = "false"),
        @ClusterConfigProperty(key = ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
        @ClusterConfigProperty(key = TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, value = "200")
    }
)
public class BaseQuotaTest {

    private final ClusterInstance clusterInstance;

    BaseQuo

    @ClusterTest
    public void testThrottledProducerConsumer() {
        int numRecords = 100;

    }

    public int produceUntilThrottled(Producer producer, int maxRecords, boolean waitForRequestCompletion) {
        int numProduced = 0;
        boolean throttled = false;
        KafkaMetric metric = throttleMetric(QuotaType.PRODUCE, producerClientId);
        do {
            byte[] payload = Integer.toString(numProduced).getBytes();
            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>(topic, null, null, payload)
            );
            numProduced += 1;
            // 等待当前请求完成或 metric 表明被 throttled（根据 waitForRequestCompletion 决定）
            do {
                throttled = (metric != null && metricValue(metric) > 0.0);
                if (future.isDone()) break;
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } while (!future.isDone() && (!throttled || waitForRequestCompletion));
        } while (numProduced < maxRecords && !throttled);
        return numProduced;
    }

    throttleMetric(QuotaType quotaType, String clientId) {
        // Implementation to retrieve the throttle metric based on quotaType and clientId

    }
}
