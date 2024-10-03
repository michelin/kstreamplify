package com.michelin.kstreamplify;

import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OverridePropertiesTest extends KafkaStreamsStarterTest {

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

    /**
     * Overwrite the default storage path.
     *
     * @return the new/overwrite properties
     */
    @Override
    protected HashMap<String, String> getSpecificProperties() {
        HashMap<String, String> propertiesMap = new HashMap<>();
        propertiesMap.put(STATE_DIR_CONFIG, SPECIFIC_STORAGE_PATH);
        
        return propertiesMap;
    }

    /**
     * Test when the default storage path is override.
     */
    @Test
    void shouldValidateStorageDirHasBeenOverride() {
        Properties properties = KafkaStreamsExecutionContext.getProperties();
        Assertions.assertEquals(SPECIFIC_STORAGE_PATH, properties.getProperty(STATE_DIR_CONFIG));
    }

}