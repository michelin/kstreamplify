package com.michelin.kstreamplify.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

class TopicWithSerdesUnitTest {

    @Test
    void shouldCreateTopicWithSerde() {
        TopicWithSerde<String, String> topicWithSerde = new TopicWithSerde<>("INPUT_TOPIC",
            Serdes.String(), Serdes.String());

        assertEquals("INPUT_TOPIC", topicWithSerde.getUnPrefixedName());
    }
}
