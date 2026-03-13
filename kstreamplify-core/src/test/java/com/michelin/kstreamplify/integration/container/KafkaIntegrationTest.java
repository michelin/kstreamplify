/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.integration.container;

import static com.michelin.kstreamplify.property.PropertiesUtils.KAFKA_PROPERTIES_PREFIX;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.property.PropertiesUtils;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Base class for Kafka integration tests. */
@Slf4j
public abstract class KafkaIntegrationTest {
    protected static final String CONFLUENT_PLATFORM_VERSION = "8.0.3";
    protected static final Network NETWORK = Network.newNetwork();
    protected final HttpClient httpClient = HttpClient.newBuilder().build();
    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected static KafkaStreamsInitializer initializer;
    private static final String KAFKA_PREFIX = "kafka.properties.";

    @Container
    protected static ConfluentKafkaContainer broker = new ConfluentKafkaContainer(
                    DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
            .withNetwork(NETWORK)
            .withNetworkAliases("broker");

    @Container
    protected static GenericContainer<?> schemaRegistry = new GenericContainer<>(
                    DockerImageName.parse("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION))
            .dependsOn(broker)
            .withNetwork(NETWORK)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:9093")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    protected static void createTopics(String bootstrapServers, TopicPartition... topicPartitions) {
        createTopics(bootstrapServers, null, topicPartitions);
    }

    protected static void createTopics(
            String bootstrapServers, Map<String, String> configs, TopicPartition... topicPartitions) {
        var newTopics = Arrays.stream(topicPartitions)
                .map(topicPartition ->
                        new NewTopic(topicPartition.topic(), topicPartition.partition(), (short) 1).configs(configs))
                .toList();
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.createTopics(newTopics);
        }
    }

    public static Properties getKafkaStreamProperties() {
        return withKafkaPrefix(Map.of(
                BOOTSTRAP_SERVERS_CONFIG,
                broker.getBootstrapServers(),
                SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl(),
                COMMIT_INTERVAL_MS_CONFIG,
                "1",
                STATE_DIR_CONFIG,
                "/tmp/kstreamplify/kstreamplify-core-test"));
    }

    public static Properties getKafkaGlobalProperties() {
        Properties properties = new Properties();

        properties.put(BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers());
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());

        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(SPECIFIC_AVRO_READER_CONFIG, "true");

        return properties;
    }

    private static Properties withKafkaPrefix(Map<String, ?> configs) {
        Properties props = new Properties();
        configs.forEach((k, v) -> props.put(KAFKA_PREFIX + k, v));
        return props;
    }

    private static String schemaRegistryUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort();
    }

    public static <K, V> void produceRecordToTopic(List<ProducerRecord<K, V>> records, Properties properties) {
        try (KafkaProducer<K, V> producer = new KafkaProducer<>(properties)) {
            for (ProducerRecord<K, V> record : records) {
                producer.send(record);
            }
            producer.flush();
        }
    }

    public static <K, V> List<ConsumerRecord<K, V>> readAllRecordsFromTopic(
            String topic, Properties properties, int expectedNumberOfRecords) {
        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            final int pollIntervalMs = 500;
            List<ConsumerRecord<K, V>> consumerRecords = new ArrayList<>();
            int totalPollTimeMs = 0;
            while (totalPollTimeMs < 30000 && consumerRecords.size() < expectedNumberOfRecords) {
                totalPollTimeMs += pollIntervalMs;
                final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollIntervalMs));
                for (final ConsumerRecord<K, V> record : records) {
                    consumerRecords.add(record);
                }
            }
            return consumerRecords;
        }
    }

    protected void waitingForKafkaStreamsToStart() throws InterruptedException {
        while (!initializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)) {
            log.info("Waiting for Kafka Streams to start...");
            Thread.sleep(2000); // NOSONAR
        }
    }

    protected void waitingForLocalStoreToReachOffset(Map<String, Map<Integer, Long>> topicPartitionOffset)
            throws InterruptedException {

        while (hasLag(topicPartitionOffset)) {
            log.info(
                    "Waiting for local stores {} to reach offsets",
                    topicPartitionOffset.keySet().stream().toList());
            Thread.sleep(5000); // NOSONAR
        }
    }

    private boolean hasLag(Map<String, Map<Integer, Long>> topicPartitionOffset) {
        Map<String, Map<Integer, LagInfo>> currentLag =
                initializer.getKafkaStreams().allLocalStorePartitionLags();

        return !topicPartitionOffset.entrySet().stream()
                .allMatch(topicPartitionOffsetEntry -> topicPartitionOffsetEntry.getValue().entrySet().stream()
                        .anyMatch(partitionOffsetEntry -> currentLag
                                        .get(topicPartitionOffsetEntry.getKey())
                                        .get(partitionOffsetEntry.getKey())
                                        .currentOffsetPosition()
                                == partitionOffsetEntry.getValue()));
    }

    /**
     * Define a KafkaStreamsInitializer stub for testing.
     *
     * <p>This stub allows to override some properties of the application.properties file or to set some properties
     * dynamically from Testcontainers.
     */
    public static class KafkaStreamInitializerStub extends KafkaStreamsInitializer {
        public KafkaStreamInitializerStub(
                KafkaStreamsStarter kafkaStreamsStarter, Integer serverPort, Properties additionalProperties) {
            super(kafkaStreamsStarter);
            this.serverPort = serverPort;
            this.properties.putAll(additionalProperties);

            Properties convertedAdditionalProperties = new Properties();
            convertedAdditionalProperties.putAll(additionalProperties);
            kafkaProperties.putAll(
                    PropertiesUtils.extractSubProperties(convertedAdditionalProperties, KAFKA_PROPERTIES_PREFIX, true));
            KafkaStreamsExecutionContext.registerProperties(kafkaProperties);
            KafkaStreamsExecutionContext.setSerdesConfig(kafkaProperties.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> String.valueOf(e.getKey()),
                            e -> String.valueOf(e.getValue()),
                            (prev, next) -> next,
                            HashMap::new)));

            this.hostInfo = new HostInfo(hostInfo.host(), serverPort);
            KafkaStreamsExecutionContext.getProperties()
                    .put(APPLICATION_SERVER_CONFIG, "%s:%s".formatted(hostInfo.host(), hostInfo.port()));
        }
    }
}
