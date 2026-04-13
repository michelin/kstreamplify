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
import java.util.List;
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

    /**
     * Deduplicates records from the input stream based on a computed key derived from each record.
     *
     * <p>The provided {@code deduplicationHeadersExtractor} generates a list of String values that together form a
     * unique identifier for a record. Records with the same identifier within the given time window are considered
     * duplicates.
     *
     * <p>A window store is used to track seen identifiers for the specified {@code windowDuration}.
     *
     * <p><b>Note:</b> This method uses internally generated store and repartition names. It should not be used multiple
     * times in the same topology. In such cases, use {@link DeduplicationUtils#deduplicateWithHeaders(StreamsBuilder,
     * KStream, String, String, Duration, List)}.
     *
     * @param streamsBuilder the {@link StreamsBuilder} used to build the topology
     * @param initialStream the input stream to deduplicate (must have String keys)
     * @param windowDuration the time window during which duplicates are filtered
     * @param deduplicationHeadersList list of header names to extract from each record for deduplication. The
     *     combination of these header values forms the unique identifier for deduplication.
     * @param <V> the value type of the stream
     * @return a deduplicated stream containing {@link ProcessingResult}
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateWithHeaders(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            Duration windowDuration,
            List<String> deduplicationHeadersList) {

        return deduplicateWithHeaders(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration,
                deduplicationHeadersList);
    }

    /**
     * Deduplicates records from the input stream based on a computed key derived from each record.
     *
     * <p>The {@code deduplicationHeadersExtractor} produces a list of String values used to build a unique identifier
     * for each record. Records sharing the same identifier within the configured time window are considered duplicates.
     *
     * <p>This variant allows specifying custom state store and repartition names, making it suitable for reuse within
     * the same topology.
     *
     * @param streamsBuilder the {@link StreamsBuilder} used to build the topology
     * @param initialStream the input stream to deduplicate (must have String keys)
     * @param storeName the name of the state store used for deduplication
     * @param repartitionName the name of the repartition topic
     * @param windowDuration the time window during which duplicates are filtered
     * @param deduplicationHeadersList list of header names to extract from each record for deduplication. The
     *     combination of these header values forms the unique identifier for deduplication.
     * @param <V> the value type of the stream
     * @return a deduplicated stream containing {@link ProcessingResult}
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateWithHeaders(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration,
            List<String> deduplicationHeadersList) {

        StoreBuilder<WindowStore<String, String>> dedupWindowStore = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false),
                Serdes.String(),
                Serdes.String());
        streamsBuilder.addStateStore(dedupWindowStore);

        var repartitioned =
                initialStream.repartition(Repartitioned.with(Serdes.String(), SerdesUtils.<V>getValueSerdes())
                        .withName(repartitionName));
        return repartitioned.process(
                () -> new DedupHeadersProcessor<>(storeName, windowDuration, deduplicationHeadersList), storeName);
    }
}
