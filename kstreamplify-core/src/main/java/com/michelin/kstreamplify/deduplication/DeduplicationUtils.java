/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.deduplication;

import com.michelin.kstreamplify.error.ProcessingResult;
import com.michelin.kstreamplify.serde.SerdesUtils;
import java.time.Duration;
import java.util.function.Function;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

/** Deduplication utility class. Only streams with String keys are supported. */
public final class DeduplicationUtils {
    private static final String DEFAULT_DEDUP_NAME = "Dedup_";
    private static final String DEFAULT_WINDOWSTORE = "WindowStore";
    private static final String DEFAULT_REPARTITION = "Repartition";

    private DeduplicationUtils() {}

    /**
     * Deduplicate the input stream on the input key using a window store for the given period of time. This constructor
     * should not be used if using the deduplicator multiple times in the same topology.
     *
     * @param streamsBuilder Stream builder instance for topology editing
     * @param initialStream Stream containing the events that should be deduplicated
     * @param windowDuration Window of time on which we should watch out for duplicates
     * @param <V> Generic Type of the Stream value. Key type is not implemented because using anything other than a
     *     String as the key is retarded. You can quote me on this.
     * @return KStream with a processingResult
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateKeys(
            StreamsBuilder streamsBuilder, KStream<String, V> initialStream, Duration windowDuration) {

        return deduplicateKeys(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration);
    }

    /**
     * Deduplicate the input stream on the input key using a window store for the given period of time.
     *
     * @param streamsBuilder Stream builder instance for topology editing
     * @param initialStream Stream containing the events that should be deduplicated
     * @param storeName State store name
     * @param repartitionName Repartition topic name
     * @param windowDuration Window of time to keep in the window store
     * @param <V> Generic Type of the Stream value. Key type is not implemented because using anything other than a
     *     String as the key is retarded. You can quote me on this.
     * @return Resulting de-duplicated Stream
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateKeys(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration) {

        StoreBuilder<WindowStore<String, String>> dedupWindowStore = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false),
                Serdes.String(),
                Serdes.String());
        streamsBuilder.addStateStore(dedupWindowStore);

        var repartitioned =
                initialStream.repartition(Repartitioned.with(Serdes.String(), SerdesUtils.<V>getValueSerdes())
                        .withName(repartitionName));
        return repartitioned.process(() -> new DedupKeyProcessor<>(storeName, windowDuration), storeName);
    }

    /**
     * Deduplicate the input stream on the input key and value using a window store for the given period of time. This
     * constructor should not be used if using the deduplicator multiple times in the same topology.
     *
     * @param streamsBuilder Stream builder instance for topology editing
     * @param initialStream Stream containing the events that should be deduplicated
     * @param windowDuration Window of time on which we should watch out for duplicates
     * @param <V> Generic Type of the Stream value. Key type is not implemented because using anything other than a
     *     String as the key is retarded. You can quote me on this.
     * @return KStream with a processingResult
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateKeyValues(
            StreamsBuilder streamsBuilder, KStream<String, V> initialStream, Duration windowDuration) {

        return deduplicateKeyValues(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration);
    }

    /**
     * Deduplicate the input stream on the input key and Value using a window store for the given period of time. The
     * input stream should have a String key.
     *
     * @param streamsBuilder Stream builder instance for topology editing
     * @param initialStream Stream containing the events that should be deduplicated
     * @param storeName State store name
     * @param repartitionName Repartition topic name
     * @param windowDuration Window of time to keep in the window store
     * @param <V> Generic Type of the Stream value. Key type is not implemented because using anything other than a
     *     String as the key is retarded. You can quote me on this.
     * @return Resulting de-duplicated Stream
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateKeyValues(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration) {

        StoreBuilder<WindowStore<String, V>> dedupWindowStore = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false),
                Serdes.String(),
                SerdesUtils.getValueSerdes());
        streamsBuilder.addStateStore(dedupWindowStore);

        var repartitioned =
                initialStream.repartition(Repartitioned.with(Serdes.String(), SerdesUtils.<V>getValueSerdes())
                        .withName(repartitionName));
        return repartitioned.process(() -> new DedupKeyValueProcessor<>(storeName, windowDuration), storeName);
    }

    /**
     * Deduplicate the input stream by applying the deduplicationKeyExtractor function on each record to generate a
     * unique signature for the record. Uses a window store for the given period of time. The input stream should have a
     * String key. This constructor should not be used if using the deduplicator multiple times in the same topology.
     * Use {@link DeduplicationUtils#deduplicateWithPredicate(StreamsBuilder, KStream, String storeName, String
     * repartitionName, Duration, Function)} in this scenario.
     *
     * @param streamsBuilder Stream builder instance for topology editing
     * @param initialStream Stream containing the events that should be deduplicated
     * @param windowDuration Window of time to keep in the window store
     * @param deduplicationKeyExtractor Function that should extract a deduplication key in String format. This key acts
     *     like a comparison vector. A recommended approach is to concatenate all necessary fields in String format to
     *     provide a unique identifier for comparison between records.
     * @param <V> Generic Type of the Stream value. Key type is not implemented because using anything other than a
     *     String as the key is retarded. You can quote me on this.
     * @return Resulting de-duplicated Stream
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateWithPredicate(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            Duration windowDuration,
            Function<V, String> deduplicationKeyExtractor) {
        return deduplicateWithPredicate(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration,
                deduplicationKeyExtractor);
    }

    /**
     * Deduplicate the input stream by applying the deduplicationKeyExtractor function on each record to generate a
     * unique signature for the record. Uses a window store for the given period of time. The input stream should have a
     * String key.
     *
     * @param streamsBuilder Stream builder instance for topology editing
     * @param initialStream Stream containing the events that should be deduplicated
     * @param storeName State store name
     * @param repartitionName Repartition topic name
     * @param windowDuration Window of time to keep in the window store
     * @param deduplicationKeyExtractor Function that should extract a deduplication key in String format. This key acts
     *     like a comparison vector. A recommended approach is to concatenate all necessary fields in String format to
     *     provide a unique identifier for comparison between records.
     * @param <V> Generic Type of the Stream value. Key type is not implemented because using anything other than a
     *     String as the key is retarded. You can quote me on this.
     * @return Resulting de-duplicated Stream
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateWithPredicate(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration,
            Function<V, String> deduplicationKeyExtractor) {

        StoreBuilder<WindowStore<String, V>> dedupWindowStore = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false),
                Serdes.String(),
                SerdesUtils.getValueSerdes());
        streamsBuilder.addStateStore(dedupWindowStore);

        var repartitioned =
                initialStream.repartition(Repartitioned.with(Serdes.String(), SerdesUtils.<V>getValueSerdes())
                        .withName(repartitionName));
        return repartitioned.process(
                () -> new DedupWithPredicateProcessor<>(storeName, windowDuration, deduplicationKeyExtractor),
                storeName);
    }
}
