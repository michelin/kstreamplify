package com.michelin.kstreamplify.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Test for TopicUtils.
 */
public class TopicUtilsTest {

    @Test
    void shouldRemapTopic() {
        Properties properties = new Properties();
        properties.put("topic.remap.myTopic", "myRemappedTopic");

        KafkaStreamsExecutionContext.setProperties(properties);

        String remappedTopic = TopicUtils.remapAndPrefix("myTopic", "");

        assertEquals("myRemappedTopic", remappedTopic);
    }
}
