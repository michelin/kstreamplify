package com.michelin.kafka.streams.starter.init;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

public class SerdesUtils {
    private SerdesUtils() { }

    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdesForKey() {
        return getSerdes(true);
    }

    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdesForValue() {
        return getSerdes(false);
    }

    private static <T extends SpecificRecord>SpecificAvroSerde<T> getSerdes(boolean isSerdeForKey){
        SpecificAvroSerde<T> serdes = new SpecificAvroSerde<>();
        serdes.configure(KafkaStreamsExecutionContext.getSerdesConfig(), isSerdeForKey);
        return serdes;
    }

    /*public static SpecificAvroSerde<GenericErrorModel> getErrorSerde() {
        SpecificAvroSerde<GenericErrorModel> serde = new SpecificAvroSerde<>();
        serde.configure(StreamExecutionContext.getSerdesConfig(), false);
        return serde;
    }*/
}
