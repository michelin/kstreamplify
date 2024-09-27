package com.michelin.kstreamplify;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class ConfigErrorHandlerTest extends KafkaStreamsStarterTest {

    private static final String DLQ_TOPIC = "dlqTopic";
    private static final String SPECIFIC_STORAGE_PATH = "/tmp/PersonalPath";
    
    @Override
    protected KafkaStreamsStarter getKafkaStreamsStarter() {
        return new KafkaStreamsStarter() {
            @Override
            public String dlqTopic() {
                return DLQ_TOPIC;
            }

            @Override
            public void topology(StreamsBuilder streamsBuilder) {
            }
        };
    }

    @Override
    protected String getStoragePath() {
        return SPECIFIC_STORAGE_PATH;
    }

    /**
     * Test when the default storage path is override.
     */
    @Test
    void testSpecificStoragePath() {
        Properties properties = KafkaStreamsExecutionContext.getProperties();
        Assertions.assertEquals(SPECIFIC_STORAGE_PATH, properties.getProperty(StreamsConfig.STATE_DIR_CONFIG));
    }

}