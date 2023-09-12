package com.michelin.kstreamplify.utils;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class TopicWithSerdesUnitTest {

    @Test
    void topicWithSerdes() {
        assertNotNull(new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String()));
    }

}
