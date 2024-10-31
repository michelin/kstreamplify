package com.michelin.kstreamplify;

import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.serde.TopicWithSerde;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * The main test class to extend to execute unit tests on topology.
 * It provides a {@link TopologyTestDriver} and a {@link TestOutputTopic} for the DLQ.
 */
public abstract class KafkaStreamsStarterTest {
    private static final String STATE_DIR = "/tmp/kafka-streams/";

    /**
     * The topology test driver.
     */
    protected TopologyTestDriver testDriver;

    /**
     * The dlq topic, initialized in {@link #generalSetUp()}.
     */
    protected TestOutputTopic<String, KafkaError> dlqTopic;

    /**
     * Set up topology test driver.
     */
    @BeforeEach
    void generalSetUp() {
        Properties properties = getProperties();

        KafkaStreamsExecutionContext.registerProperties(properties);
        KafkaStreamsExecutionContext.setSerdesConfig(Collections
            .singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "mock://" + getClass().getSimpleName()));

        KafkaStreamsStarter starter = getKafkaStreamsStarter();

        KafkaStreamsExecutionContext.setDlqTopicName(starter.dlqTopic());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        starter.topology(streamsBuilder);

        testDriver =
            new TopologyTestDriver(streamsBuilder.build(), properties, getInitialWallClockTime());

        dlqTopic = testDriver.createOutputTopic(KafkaStreamsExecutionContext.getDlqTopicName(),
            new StringDeserializer(), SerdesUtils.<KafkaError>getValueSerdes().deserializer());
    }

    /** 
     * Get the properties for the test.
     *
     * @return The properties for the test
     */
    private Properties getProperties() {
        Properties properties = new Properties();

        // Default properties
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR + getClass().getSimpleName());
        
        // Add specific properties or overwrite default properties
        Map<String, String> propertiesMap = getSpecificProperties();
        if (propertiesMap != null && !propertiesMap.isEmpty()) {
            properties.putAll(propertiesMap);
        }
        
        return properties;
    }
    
    /**
     * Method to override to provide the KafkaStreamsStarter to test.
     *
     * @return The KafkaStreamsStarter to test
     */
    protected abstract KafkaStreamsStarter getKafkaStreamsStarter();

    /**
     * Default base wall clock time for topology test driver.
     *
     * @return The default wall clock time as instant
     */
    protected Instant getInitialWallClockTime() {
        return Instant.ofEpochMilli(1577836800000L);
    }

    /**
     * Create/Overwrite properties.
     *
     * @return new/overwrite properties
     */
    protected Map<String, String> getSpecificProperties() {
        return Collections.emptyMap();
    }
    
    /**
     * Method to close everything properly at the end of the test.
     */
    @AfterEach
    void generalTearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(KafkaStreamsExecutionContext.getProperties().getProperty(STATE_DIR_CONFIG)));
        MockSchemaRegistry.dropScope("mock://" + getClass().getSimpleName());
    }

    /**
     * Creates an input test topic on the testDriver using the provided topicWithSerde.
     *
     * @param topicWithSerde The topic with serde used to crete the test topic
     * @param <K>            The serializable type of the key
     * @param <V>            The serializable type of the value
     * @return The corresponding TestInputTopic
     */
    protected <K, V> TestInputTopic<K, V> createInputTestTopic(TopicWithSerde<K, V> topicWithSerde) {
        return this.testDriver.createInputTopic(
                topicWithSerde.getUnPrefixedName(),
                topicWithSerde.getKeySerde().serializer(),
                topicWithSerde.getValueSerde().serializer()
        );
    }

    /**
     * Creates an input test topic on the testDriver using the provided topicWithSerde.
     *
     * @param topicWithSerde The topic with serde used to crete the test topic
     * @param <K>            The serializable type of the key
     * @param <V>            The serializable type of the value
     * @return The corresponding TestInputTopic
     *
     * @deprecated Use {@link #createInputTestTopic(TopicWithSerde)}
     */
    @Deprecated(since = "1.1.0")
    protected <K, V> TestInputTopic<K, V> createInputTestTopic(
            com.michelin.kstreamplify.utils.TopicWithSerde<K, V> topicWithSerde
    ) {
        return createInputTestTopic(
            new TopicWithSerde<>(
                topicWithSerde.getUnPrefixedName(),
                topicWithSerde.getKeySerde(),
                topicWithSerde.getValueSerde()
            )
        );
    }

    /**
     * Creates an output test topic on the testDriver using the provided topicWithSerde.
     *
     * @param topicWithSerde The topic with serde used to crete the test topic
     * @param <K>            The serializable type of the key
     * @param <V>            The serializable type of the value
     * @return The corresponding TestOutputTopic
     */
    protected <K, V> TestOutputTopic<K, V> createOutputTestTopic(TopicWithSerde<K, V> topicWithSerde) {
        return this.testDriver.createOutputTopic(
                topicWithSerde.getUnPrefixedName(),
                topicWithSerde.getKeySerde().deserializer(),
                topicWithSerde.getValueSerde().deserializer()
        );
    }

    /**
     * Creates an output test topic on the testDriver using the provided topicWithSerde.
     *
     * @param topicWithSerde The topic with serde used to crete the test topic
     * @param <K>            The serializable type of the key
     * @param <V>            The serializable type of the value
     * @return The corresponding TestOutputTopic
     *
     * @deprecated Use {@link #createOutputTestTopic(TopicWithSerde)}
     */
    @Deprecated(since = "1.1.0")
    protected <K, V> TestOutputTopic<K, V> createOutputTestTopic(
        com.michelin.kstreamplify.utils.TopicWithSerde<K, V> topicWithSerde
    ) {
        return createOutputTestTopic(
            new TopicWithSerde<>(
                topicWithSerde.getUnPrefixedName(),
                topicWithSerde.getKeySerde(),
                topicWithSerde.getValueSerde()
            )
        );
    }
}
