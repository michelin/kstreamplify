package com.michelin.kstreamplify.utils;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

public class TopicWithSerdesUnitTest {

    @Test
    public void topicWithSerdes() {
        new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());
    }

}
