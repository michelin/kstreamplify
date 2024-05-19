package com.michelin.kstreamplify.serde;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.NoArgsConstructor;
import org.apache.avro.specific.SpecificRecord;

/**
 * The Serde utils class.
 */
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class SerdesUtils {
    /**
     * Return a key serde for a requested class.
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getKeySerdes() {
        return getSerdes(true);
    }

    /**
     * Return a value serdes for a requested class.
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerdes() {
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
