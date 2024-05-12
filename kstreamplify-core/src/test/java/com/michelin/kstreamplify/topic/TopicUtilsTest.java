package com.michelin.kstreamplify.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TopicUtilsTest {

    @Test
    void shouldRemapTopic() {
        Properties properties = new Properties();
        properties.put("topic.remap.myTopic", "myRemappedTopic");

        KafkaStreamsExecutionContext.setProperties(properties);

        String remappedTopic = TopicUtils.remapAndPrefix("myTopic", "");

        assertEquals("myRemappedTopic", remappedTopic);
    }

    @Test
    void shouldRemapAndPrefixTopic() {
        Properties properties = new Properties();
        properties.put("topic.remap.myTopic", "myRemappedTopic");
        properties.put("prefix.myNamespace", "myNamespacePrefix.");

        KafkaStreamsExecutionContext.setProperties(properties);

        String remappedTopic = TopicUtils.remapAndPrefix("myTopic", "myNamespace");

        assertEquals("myNamespacePrefix.myRemappedTopic", remappedTopic);
    }
}
