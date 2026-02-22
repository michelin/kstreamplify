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
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.property.PropertiesUtils;
import java.net.http.HttpClient;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
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
        var newTopics = Arrays.stream(topicPartitions)
                .map(topicPartition -> new NewTopic(topicPartition.topic(), topicPartition.partition(), (short) 1))
                .toList();
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.createTopics(newTopics);
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
