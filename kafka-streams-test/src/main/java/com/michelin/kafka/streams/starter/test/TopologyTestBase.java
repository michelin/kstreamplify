package com.michelin.kafka.streams.starter.test;

import com.michelin.kafka.streams.starter.init.KafkaStreamsExecutionContext;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.michelin.kafka.streams.starter.init.KafkaStreamsExecutionContext.PREFIX;

public abstract class TopologyTestBase {

    public final static String DLQ_TOPIC = "DLQ_TOPIC";
    private static final String DIRECTORY = "C:/tmp/kafka-streams";

    protected TopologyTestDriver testDriver;

    protected String schemaRegistryScope;

    @BeforeEach
    void setUp() {

        schemaRegistryScope = this.getClass().getName();
        String mockSchemaRegistryUrl = "mock://" + schemaRegistryScope;
        Map<String, String> serdeConfig = Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSchemaRegistryUrl);

        StreamsBuilder builder = new StreamsBuilder();
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, mockSchemaRegistryUrl);

        Properties resetPrefixProperties = new Properties();
        resetPrefixProperties.setProperty(PREFIX, "");
        KafkaStreamsExecutionContext.registerProperties(resetPrefixProperties);
        KafkaStreamsExecutionContext.setDlqTopicName(DLQ_TOPIC);
        KafkaStreamsExecutionContext.setSerdesConfig(serdeConfig);
        KafkaStreamsExecutionContext.setNumPartition(1);

        applyProperties(properties);
        applyTopology(builder);
        var getInitialDriverDate = getDriverDate();

        var topology = builder.build();
        testDriver = new TopologyTestDriver(topology, properties, getInitialDriverDate);
    }

    /**
     * Implement this method to inject the topology you want to use for your test (~MyTopology.addMyTopology(builder))
     *
     * @param builder
     */
    protected abstract void applyTopology(StreamsBuilder builder);

    /**
     * Implement this method to override the default mocked date for streams events
     * Default value is Wednesday 1 January 2020 00:00:00 GMT
     *
     */
    protected Instant getDriverDate() {
        return Instant.ofEpochMilli(1577836800000L);
    }

    /**
     * Allows the developer to override properties for the test using properties.setProperties
     *
     * @param properties
     */
    protected void applyProperties(Properties properties) {
        Path tempDirectory;
        try {
            tempDirectory = Files.createTempDirectory("test");
            tempDirectory.toFile().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, tempDirectory.toString());
    }

    @AfterEach
    void tearDown() {
        try {
            testDriver.close();
        } catch (Exception e) {
            FileUtils.deleteQuietly(new File(DIRECTORY)); //there is a bug on Windows that does not delete the state directory properly. In order for the test to pass, the directory must be deleted manually
        }
        MockSchemaRegistry.dropScope(schemaRegistryScope);
    }

}
