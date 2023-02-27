package com.michelin.kafka.streams.starter.commons.utils;
import com.michelin.kafka.streams.starter.initializer.KafkaStreamsExecutionContext;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

/**
 * Some Serdes utils method	
 */
public class SerdesUtils {
    /**
     * Return a serdes for requested class. Only works for value.	
     * Deprecated, use {@link #getSerdesForValue() getSerdesForValue} or {@link #getSerdesForKey() getSerdesForKey} instead	
     *
     * @param <T> Class of requested Serdes	
     * @return a serdes for Requested Class	
     */
    @Deprecated
    public static <T extends SpecificRecord>SpecificAvroSerde<T> getSerdes() {
        return getSerdesForValue();
    }
    /**
     * Return a key serdes for requested class	
     * @param <T> Class of requested Serdes	
     * @return a serdes for Requested Class	
     */
    public static <T extends SpecificRecord>SpecificAvroSerde<T> getSerdesForKey() {
        return getSerdes(true);
    }
    /**
     * Return a value serdes for requested class	
     * @param <T> Class of requested Serdes	
     * @return a serdes for Requested Class	
     */
    public static <T extends SpecificRecord>SpecificAvroSerde<T> getSerdesForValue() {
        return getSerdes(false);
    }
    /**
     *
     * @param isSerdeForKey Is the serdes for a key avro.	
     * @param <T>Class of requested Serdes	
     * @return a serdes for Requested Class	
     */
    private static <T extends SpecificRecord>SpecificAvroSerde<T> getSerdes(boolean isSerdeForKey){
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<T>();
        serde.configure(KafkaStreamsExecutionContext.getSerdesConfig(), isSerdeForKey);
        return serde;
    }
}
