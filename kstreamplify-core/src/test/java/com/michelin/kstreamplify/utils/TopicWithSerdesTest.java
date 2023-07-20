package com.michelin.kstreamplify.utils;

import com.michelin.kstreamplify.avro.KafkaError;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class TopicWithSerdesTest<K, V> extends TopicWithSerde<K, V> {

    private TopicWithSerdesTest(String name, String appName, Serde<K> keySerde, Serde<V> valueSerde) {
        super(name, appName, keySerde, valueSerde);
    }

    public static TopicWithSerdesTest<String, String> outputTopicWithSerdes() {
        return new TopicWithSerdesTest<>("OUTPUT_TOPIC", "APP_NAME", Serdes.String(), Serdes.String());
    }

    public static TopicWithSerdesTest<String, KafkaError> inputTopicWithSerdes() {
        return new TopicWithSerdesTest<>("INPUT_TOPIC", "APP_NAME", Serdes.String(), SerdesUtils.getSerdesForValue());
    }
}
