package com.michelin.kstreamplify.utils;

import com.michelin.kstreamplify.avro.KafkaError;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class TopicWithSerdesTestHelper<K, V> extends TopicWithSerde<K, V> {

    private TopicWithSerdesTestHelper(String name, String appName, Serde<K> keySerde, Serde<V> valueSerde) {
        super(name, appName, keySerde, valueSerde);
    }

    public static TopicWithSerdesTestHelper<String, String> outputTopicWithSerdes() {
        return new TopicWithSerdesTestHelper<>("OUTPUT_TOPIC", "APP_NAME", Serdes.String(), Serdes.String());
    }

    public static TopicWithSerdesTestHelper<String, KafkaError> inputTopicWithSerdes() {
        return new TopicWithSerdesTestHelper<>("INPUT_TOPIC", "APP_NAME", Serdes.String(), SerdesUtils.getSerdesForValue());
    }

}
