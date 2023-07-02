package io.github.michelin.kstreamplify;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.github.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import io.github.michelin.kstreamplify.utils.TopicWithSerde;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

/**
 * The main test class to extend to execute unit tests on topology
 */
public abstract class KafkaStreamsStarterTest {
    private static final String STATE_DIR = "/tmp/kafka-streams";

    /**
     * The default DLQ topic name used for testing
     */
    protected static final String DLQ_TOPIC = "DLQ_TOPIC";

    /**
     * The topology test driver
     */
    protected TopologyTestDriver testDriver;

    /**
     * Set up topology test driver
     */
    @BeforeEach
    void generalSetUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        KafkaStreamsExecutionContext.registerProperties(properties);
        KafkaStreamsExecutionContext.setDlqTopicName(DLQ_TOPIC);
        KafkaStreamsExecutionContext.setSerdesConfig(Collections
                .singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getName()));

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topology(streamsBuilder);

        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, getInitialWallClockTime());
    }

    /**
     * Method to create the topology to test
     * @param streamsBuilder to build the topology of the stream
     */
    protected abstract void topology(StreamsBuilder streamsBuilder);

    /**
     * Default base wall clock time for topology test driver
     * @return The default wall clock time as instant
     */
    protected Instant getInitialWallClockTime() {
        return Instant.ofEpochMilli(1577836800000L);
    }

    /**
     * Method to close everything properly at the end of the test
     */
    @AfterEach
    void generalTearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope("mock://" + getClass().getName());
    }
    
    protected <K, V> TestInputTopic<K, V> createInputTestTopic(TopicWithSerde<K, V> topicWithSerde) {
        return this.testDriver.createInputTopic(topicWithSerde.getUnPrefixedName(), topicWithSerde.getKeySerde().serializer(), topicWithSerde.getValueSerde().serializer());
    }

    protected <K, V> TestOutputTopic<K, V> createOutputTestTopic(TopicWithSerde<K, V> topicWithSerde) {
        return this.testDriver.createOutputTopic(topicWithSerde.getUnPrefixedName(), topicWithSerde.getKeySerde().deserializer(), topicWithSerde.getValueSerde().deserializer());
    }
}
