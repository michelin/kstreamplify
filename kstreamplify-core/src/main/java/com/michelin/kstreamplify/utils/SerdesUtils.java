package com.michelin.kstreamplify.utils;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

/**
 * The Serdes utils class.
 *
 * @deprecated Use {@link com.michelin.kstreamplify.serde.SerdesUtils}.
 */
@Deprecated(forRemoval = true, since = "1.0.3")
public final class SerdesUtils {
    private SerdesUtils() {
    }

    /**
     * Return a key serdes for a requested class.
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdesForKey() {
        return getSerdes(true);
    }

    /**
     * Return a value serdes for a requested class.
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdesForValue() {
        return getSerdes(false);
    }

    /**
     * Return a serdes for a requested class.
     *
     * @param isSerdeForKey Is the serdes for a key or a value
     * @param <T>           The class of requested serdes
     * @return a serdes for requested class
     */
    private static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdes(
        boolean isSerdeForKey) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(KafkaStreamsExecutionContext.getSerdeConfig(), isSerdeForKey);
        return serde;
    }
}