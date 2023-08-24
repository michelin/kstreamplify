package com.michelin.kstreamplify;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.utils.SerdesUtils;
import com.michelin.kstreamplify.utils.TopicWithSerde;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

/**
 * <p>The main test class to extend to execute unit tests on topology</p>
 * <p>It provides a {@link TopologyTestDriver} and a {@link TestOutputTopic} for the DLQ</p>
 */
public abstract class KafkaStreamsStarterTest {
    private static final String STATE_DIR = "/tmp/kafka-streams/";

    /**
     * The topology test driver
     */
    protected TopologyTestDriver testDriver;

    /**
     * The dlq topic, initialized in {@link #generalSetUp()}
     */
    protected TestOutputTopic<String, KafkaError> dlqTopic;

    /**
     * Set up topology test driver
     */
    @BeforeEach
    void generalSetUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR + getClass().getName());

        KafkaStreamsExecutionContext.registerProperties(properties);
        KafkaStreamsExecutionContext.setSerdesConfig(Collections
                .singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getName()));

        var starter = getKafkaStreamsStarter();

        KafkaStreamsExecutionContext.setDlqTopicName(starter.dlqTopic());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        starter.topology(streamsBuilder);

        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, getInitialWallClockTime());

        dlqTopic = testDriver.createOutputTopic(KafkaStreamsExecutionContext.getDlqTopicName(), new StringDeserializer(), SerdesUtils.<KafkaError>getSerdesForValue().deserializer());
    }

    /**
     * Method to override to provide the KafkaStreamsStarter to test
     *
     * @return The KafkaStreamsStarter to test
     */
    protected abstract KafkaStreamsStarter getKafkaStreamsStarter();

    /**
     * Default base wall clock time for topology test driver
     *
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
        Files.deleteIfExists(Paths.get(STATE_DIR + getClass().getName()));
        MockSchemaRegistry.dropScope("mock://" + getClass().getName());
    }

    /**
     * Creates an input test topic on the testDriver using the provided topicWithSerde
     *
     * @param topicWithSerde The topic with serde used to crete the test topic
     * @param <K>            The serializable type of the key
     * @param <V>            The serializable type of the value
     * @return The corresponding TestInputTopic
     */
    protected <K, V> TestInputTopic<K, V> createInputTestTopic(TopicWithSerde<K, V> topicWithSerde) {
        return this.testDriver.createInputTopic(topicWithSerde.getUnPrefixedName(), topicWithSerde.getKeySerde().serializer(), topicWithSerde.getValueSerde().serializer());
    }

    /**
     * Creates an output test topic on the testDriver using the provided topicWithSerde
     *
     * @param topicWithSerde The topic with serde used to crete the test topic
     * @param <K>            The serializable type of the key
     * @param <V>            The serializable type of the value
     * @return The corresponding TestOutputTopic
     */
    protected <K, V> TestOutputTopic<K, V> createOutputTestTopic(TopicWithSerde<K, V> topicWithSerde) {
        return this.testDriver.createOutputTopic(topicWithSerde.getUnPrefixedName(), topicWithSerde.getKeySerde().deserializer(), topicWithSerde.getValueSerde().deserializer());
    }
}
