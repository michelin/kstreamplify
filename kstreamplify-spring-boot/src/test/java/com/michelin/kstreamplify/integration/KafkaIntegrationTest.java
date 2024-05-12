package com.michelin.kstreamplify.integration;

import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.FileSystemUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

@Slf4j
abstract class KafkaIntegrationTest {
    protected static final String STATE_DIR = "/tmp/kstreamplify-test";

    @Autowired
    protected KafkaStreamsInitializer initializer;

    @Autowired
    protected TestRestTemplate restTemplate;

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName
        .parse("confluentinc/cp-kafka:7.6.1"))
        .withKraft();

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("kafka.properties." + BOOTSTRAP_SERVERS_CONFIG,
            kafka::getBootstrapServers);
    }

    protected static void createTopics(String... topics) {
        var newTopics = Arrays.stream(topics)
            .map(topic -> new NewTopic(topic, 1, (short) 1))
            .toList();
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(newTopics);
        }
    }

    protected void waitingForKafkaStreamsToStart() throws InterruptedException {
        while (!initializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)) {
            log.info("Waiting for Kafka Streams to start...");
            Thread.sleep(2000);
        }
    }
}
