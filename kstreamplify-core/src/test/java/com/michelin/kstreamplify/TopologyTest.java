package com.michelin.kstreamplify;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarterTopologyTestHelper;
import com.michelin.kstreamplify.model.TopologyExposeJsonModel;
import com.michelin.kstreamplify.services.ConvertTopology;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class TopologyTest {
    protected TopologyTestDriver testDriver;
    @Test
    void convertTopologyForRestTest() {

        Properties properties = new Properties();
        properties.setProperty("application.id", "test");
        properties.setProperty("bootstrap.servers", "mock:1234");
        properties.setProperty("state.dir", "/tmp/kafka-streams");
        KafkaStreamsExecutionContext.registerProperties(properties);
        KafkaStreamsExecutionContext.setDlqTopicName("DLQ_TOPIC");
        KafkaStreamsExecutionContext.setSerdesConfig(Collections.singletonMap("schema.registry.url", "mock://" + this.getClass().getName()));


        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsStarterTopologyTestHelper kafkaStreamsStarterTopologyTest = new KafkaStreamsStarterTopologyTestHelper();
        kafkaStreamsStarterTopologyTest.topology(streamsBuilder);
        KafkaStreamsInitializer kafkaStreamsInitializer = new KafkaStreamsInitializer();
        kafkaStreamsInitializer.init(kafkaStreamsStarterTopologyTest);
        TopologyExposeJsonModel topology = ConvertTopology.convertTopologyForRest("STREAM", kafkaStreamsInitializer.getTopology());

        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, this.getInitialWallClockTime());

        assertNotNull(topology);
        testDriver.advanceWallClockTime(Duration.ofDays(1));
    }
    private Instant getInitialWallClockTime() {
        return Instant.ofEpochMilli(1577836800000L);
    }
}
