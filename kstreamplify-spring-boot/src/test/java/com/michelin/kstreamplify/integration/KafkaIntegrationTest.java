package com.michelin.kstreamplify.integration;

import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.testcontainers.containers.Network;

@Slf4j
abstract class KafkaIntegrationTest {
    protected static final String CONFLUENT_PLATFORM_VERSION = "7.6.1";
    protected static final Network NETWORK = Network.newNetwork();

    @Autowired
    protected KafkaStreamsInitializer initializer;

    @Autowired
    protected TestRestTemplate restTemplate;

    protected static void createTopics(String bootstrapServers, String... topics) {
        var newTopics = Arrays.stream(topics)
            .map(topic -> new NewTopic(topic, 2, (short) 1))
            .toList();
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.createTopics(newTopics);
        }
    }

    protected void waitingForKafkaStreamsToStart() throws InterruptedException {
        while (!initializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)) {
            log.info("Waiting for Kafka Streams to start...");
            Thread.sleep(2000);
        }
    }

    protected void waitingForLocalStoreToReachOffset(Map<String, Map<Integer, Long>> topicPartitionOffset)
        throws InterruptedException {

        while (hasLag(topicPartitionOffset)) {
            log.info("Waiting for local stores {} to reach offsets", topicPartitionOffset.keySet().stream().toList());
            Thread.sleep(5000);
        }
    }

    private boolean hasLag(Map<String, Map<Integer, Long>> topicPartitionOffset) {
        Map<String, Map<Integer, LagInfo>> currentLag = initializer.getKafkaStreams().allLocalStorePartitionLags();

        return !topicPartitionOffset.entrySet()
            .stream()
            .allMatch(topicPartitionOffsetEntry -> topicPartitionOffsetEntry.getValue().entrySet()
                .stream()
                .anyMatch(partitionOffsetEntry -> currentLag.get(topicPartitionOffsetEntry.getKey())
                    .get(partitionOffsetEntry.getKey())
                    .currentOffsetPosition() == partitionOffsetEntry.getValue()));
    }
}
