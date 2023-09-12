package com.michelin.kstreamplify.utils;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

class TopicWithSerdesUnitTest {

    @Test
    void topicWithSerdes() {
        new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());
    }

}
