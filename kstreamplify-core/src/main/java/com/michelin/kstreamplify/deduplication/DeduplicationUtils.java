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

    /** @deprecated Since 1.7.0, use {@link #distinctKeysWithErrors(StreamsBuilder, KStream, Duration)} instead. */
    @Deprecated(since = "1.7.0", forRemoval = true)
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateKeys(
            StreamsBuilder streamsBuilder, KStream<String, V> initialStream, Duration windowDuration) {

        return distinctKeysWithErrors(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration);
    }

    /**
     * @deprecated Since 1.7.0, use {@link #distinctKeysWithErrors(StreamsBuilder, KStream, String, String, Duration)}
     *     instead.
     */
    @Deprecated(since = "1.7.0", forRemoval = true)
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateKeys(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration) {

        return distinctKeysWithErrors(streamsBuilder, initialStream, storeName, repartitionName, windowDuration);
    }

    /** See {@link #distinctKeysWithErrors(StreamsBuilder, KStream, String, String, Duration)} */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> distinctKeysWithErrors(
            StreamsBuilder streamsBuilder, KStream<String, V> initialStream, Duration windowDuration) {

        return distinctKeysWithErrors(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration);
    }

    /**
     * Deduplicates records from the input stream using the record key.
     *
     * <p>Records with identical keys within the configured time window are considered duplicates and are filtered out.
     *
     * <p>A window store is used to track seen keys during the specified {@code windowDuration}.
     *
     * @param streamsBuilder the {@link StreamsBuilder} used to build the topology
     * @param initialStream the input stream to deduplicate (must have String keys)
     * @param storeName the name of the state store used for deduplication
     * @param repartitionName the name of the repartition topic
     * @param windowDuration the time window during which duplicates are filtered
     * @param <V> the value type of the stream
     * @return a deduplicated stream containing {@link ProcessingResult}
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> distinctKeysWithErrors(
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

        return repartitioned.process(() -> new DedupKeyProcessorWithErrors<>(storeName, windowDuration), storeName);
    }

    /** See {@link #distinctKeys(StreamsBuilder, KStream, String, String, Duration)} */
    public static <V extends SpecificRecord> KStream<String, V> distinctKeys(
            StreamsBuilder streamsBuilder, KStream<String, V> initialStream, Duration windowDuration) {

        return distinctKeys(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration);
    }

    /**
     * Deduplicates records from the input stream using the record key.
     *
     * <p>Records with identical keys within the configured time window are considered duplicates and are filtered out.
     *
     * <p>A window store is used to track seen keys during the specified {@code windowDuration}.
     *
     * @param streamsBuilder the {@link StreamsBuilder} used to build the topology
     * @param initialStream the input stream to deduplicate (must have String keys)
     * @param storeName the name of the state store used for deduplication
     * @param repartitionName the name of the repartition topic
     * @param windowDuration the time window during which duplicates are filtered
     * @param <V> the value type of the stream
     * @return a deduplicated stream containing
     */
    public static <V extends SpecificRecord> KStream<String, V> distinctKeys(
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
     * @deprecated Since 1.7.0, use {@link #distinctByKeyValuesWithErrors(StreamsBuilder, KStream, Duration)} instead.
     */
    @Deprecated(since = "1.7.0", forRemoval = true)
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateKeyValues(
            StreamsBuilder streamsBuilder, KStream<String, V> initialStream, Duration windowDuration) {

        return distinctByKeyValuesWithErrors(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration);
    }

    /**
     * @deprecated Since 1.7.0, use {@link #distinctByKeyValuesWithErrors(StreamsBuilder, KStream, String, String,
     *     Duration)} instead.
     */
    @Deprecated(since = "1.7.0", forRemoval = true)
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateKeyValues(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration) {

        return distinctByKeyValuesWithErrors(streamsBuilder, initialStream, storeName, repartitionName, windowDuration);
    }

    /** See {@link #distinctByKeyValuesWithErrors(StreamsBuilder, KStream, String, String, Duration)} */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> distinctByKeyValuesWithErrors(
            StreamsBuilder streamsBuilder, KStream<String, V> initialStream, Duration windowDuration) {

        return distinctByKeyValuesWithErrors(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration);
    }

    /**
     * Deduplicates records from the input stream using both key and value.
     *
     * <p>Records with identical key-value pairs within the configured time window are considered duplicates and are
     * filtered out.
     *
     * @param streamsBuilder the {@link StreamsBuilder} used to build the topology
     * @param initialStream the input stream to deduplicate
     * @param storeName the name of the state store used for deduplication
     * @param repartitionName the name of the repartition topic
     * @param windowDuration the time window during which duplicates are filtered
     * @param <V> the value type of the stream
     * @return a deduplicated stream containing {@link ProcessingResult}
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> distinctByKeyValuesWithErrors(
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

        return repartitioned.process(
                () -> new DedupKeyValueProcessorWithErrors<>(storeName, windowDuration), storeName);
    }

    /** See {@link #distinctByKeyValues(StreamsBuilder, KStream, String, String, Duration)} */
    public static <V extends SpecificRecord> KStream<String, V> distinctByKeyValues(
            StreamsBuilder streamsBuilder, KStream<String, V> initialStream, Duration windowDuration) {

        return distinctByKeyValues(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration);
    }

    /**
     * Deduplicates records from the input stream using both key and value.
     *
     * <p>Records with identical key-value pairs within the configured time window are considered duplicates and are
     * filtered out.
     *
     * @param streamsBuilder the {@link StreamsBuilder} used to build the topology
     * @param initialStream the input stream to deduplicate
     * @param storeName the name of the state store used for deduplication
     * @param repartitionName the name of the repartition topic
     * @param windowDuration the time window during which duplicates are filtered
     * @param <V> the value type of the stream
     * @return a deduplicated stream containing
     */
    public static <V extends SpecificRecord> KStream<String, V> distinctByKeyValues(
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
     * @deprecated Since 1.7.0, use {@link #distinctByPredicateWithErrors(StreamsBuilder, KStream, Duration, Function)}
     *     instead.
     */
    @Deprecated(since = "1.7.0", forRemoval = true)
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateWithPredicate(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            Duration windowDuration,
            Function<V, String> extractor) {

        return distinctByPredicateWithErrors(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration,
                extractor);
    }

    /**
     * @deprecated Since 1.7.0, use {@link #distinctByPredicateWithErrors(StreamsBuilder, KStream, String, String,
     *     Duration, Function)} instead.
     */
    @Deprecated(since = "1.7.0", forRemoval = true)
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateWithPredicate(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration,
            Function<V, String> extractor) {

        return distinctByPredicateWithErrors(
                streamsBuilder, initialStream, storeName, repartitionName, windowDuration, extractor);
    }

    /** See {@link #distinctByPredicateWithErrors(StreamsBuilder, KStream, String, String, Duration, Function)} */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> distinctByPredicateWithErrors(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            Duration windowDuration,
            Function<V, String> extractor) {

        return distinctByPredicateWithErrors(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration,
                extractor);
    }

    /**
     * Deduplicates records from the input stream using a computed deduplication key.
     *
     * <p>The provided extractor builds a deduplication key for each record. Records with identical keys within the
     * configured time window are considered duplicates and are filtered out.
     *
     * @param streamsBuilder the {@link StreamsBuilder}
     * @param initialStream the input stream
     * @param storeName state store name
     * @param repartitionName repartition topic name
     * @param windowDuration deduplication window
     * @param extractor function building the deduplication key
     * @param <V> value type
     * @return a deduplicated stream containing {@link ProcessingResult}
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> distinctByPredicateWithErrors(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration,
            Function<V, String> extractor) {

        StoreBuilder<WindowStore<String, V>> dedupWindowStore = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false),
                Serdes.String(),
                SerdesUtils.getValueSerdes());
        streamsBuilder.addStateStore(dedupWindowStore);

        var repartitioned =
                initialStream.repartition(Repartitioned.with(Serdes.String(), SerdesUtils.<V>getValueSerdes())
                        .withName(repartitionName));

        return repartitioned.process(
                () -> new DedupWithPredicateProcessorWithErrors<>(storeName, windowDuration, extractor), storeName);
    }

    /** See {@link #distinctByPredicateWithErrors(StreamsBuilder, KStream, String, String, Duration, Function)} */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> distinctByPredicate(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            Duration windowDuration,
            Function<V, String> extractor) {

        return distinctByPredicateWithErrors(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration,
                extractor);
    }

    /**
     * Deduplicates records from the input stream using a computed deduplication key.
     *
     * <p>The provided extractor builds a deduplication key for each record. Records with identical keys within the
     * configured time window are considered duplicates and are filtered out.
     *
     * @param streamsBuilder the {@link StreamsBuilder}
     * @param initialStream the input stream
     * @param storeName state store name
     * @param repartitionName repartition topic name
     * @param windowDuration deduplication window
     * @param extractor function building the deduplication key
     * @param <V> value type
     * @return a deduplicated stream containing
     */
    public static <V extends SpecificRecord> KStream<String, V> distinctByPredicate(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration,
            Function<V, String> extractor) {

        StoreBuilder<WindowStore<String, V>> dedupWindowStore = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false),
                Serdes.String(),
                SerdesUtils.getValueSerdes());
        streamsBuilder.addStateStore(dedupWindowStore);

        var repartitioned =
                initialStream.repartition(Repartitioned.with(Serdes.String(), SerdesUtils.<V>getValueSerdes())
                        .withName(repartitionName));

        return repartitioned.process(
                () -> new DedupWithPredicateProcessor<>(storeName, windowDuration, extractor), storeName);
    }

    /**
     * @deprecated Since 1.7.0, use {@link #distinctByHeadersWithErrors(StreamsBuilder, KStream, Duration, List)}
     *     instead.
     */
    @Deprecated(since = "1.7.0", forRemoval = true)
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateWithHeaders(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            Duration windowDuration,
            List<String> deduplicationHeadersList) {

        return distinctByHeadersWithErrors(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration,
                deduplicationHeadersList);
    }

    /**
     * @deprecated since 1.7.0, use {@link #distinctByHeadersWithErrors(StreamsBuilder, KStream, String, String,
     *     Duration, List)} instead.
     */
    @Deprecated(since = "1.7.0", forRemoval = true)
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> deduplicateWithHeaders(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            String storeName,
            String repartitionName,
            Duration windowDuration,
            List<String> deduplicationHeadersList) {

        return distinctByHeadersWithErrors(
                streamsBuilder, initialStream, storeName, repartitionName, windowDuration, deduplicationHeadersList);
    }

    /** See {@link #distinctByHeadersWithErrors(StreamsBuilder, KStream, String, String, Duration, List)} */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> distinctByHeadersWithErrors(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            Duration windowDuration,
            List<String> deduplicationHeadersList) {

        return distinctByHeadersWithErrors(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration,
                deduplicationHeadersList);
    }

    /**
     * Deduplicates records from the input stream using a composite key built from the provided headers.
     *
     * <p>The {@code deduplicationHeadersList} defines which headers are used to build the deduplication key. Records
     * with identical header values within the configured time window are considered duplicates and filtered out.
     *
     * <p>A window store is used to track seen keys during the specified {@code windowDuration}.
     *
     * @param streamsBuilder the {@link StreamsBuilder} used to build the topology
     * @param initialStream the input stream to deduplicate (must have String keys)
     * @param storeName the name of the state store used for deduplication
     * @param repartitionName the name of the repartition topic
     * @param windowDuration the time window during which duplicates are filtered
     * @param deduplicationHeadersList list of header names used to build the deduplication key
     * @param <V> the value type of the stream
     * @return a deduplicated stream containing {@link ProcessingResult}
     */
    public static <V extends SpecificRecord> KStream<String, ProcessingResult<V, V>> distinctByHeadersWithErrors(
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
                () -> new DedupHeadersProcessorWithErrors<>(storeName, windowDuration, deduplicationHeadersList),
                storeName);
    }

    /** See {@link #distinctByHeaders(StreamsBuilder, KStream, String, String, Duration, List)} */
    public static <V extends SpecificRecord> KStream<String, V> distinctByHeaders(
            StreamsBuilder streamsBuilder,
            KStream<String, V> initialStream,
            Duration windowDuration,
            List<String> deduplicationHeadersList) {

        return distinctByHeaders(
                streamsBuilder,
                initialStream,
                DEFAULT_DEDUP_NAME + DEFAULT_WINDOWSTORE,
                DEFAULT_DEDUP_NAME + DEFAULT_REPARTITION,
                windowDuration,
                deduplicationHeadersList);
    }

    /**
     * Deduplicates records from the input stream using a composite key built from the provided headers.
     *
     * <p>The {@code deduplicationHeadersList} defines which headers are used to build the deduplication key. Records
     * with identical header values within the configured time window are considered duplicates and filtered out.
     *
     * <p>A window store is used to track seen keys during the specified {@code windowDuration}.
     *
     * @param streamsBuilder the {@link StreamsBuilder} used to build the topology
     * @param initialStream the input stream to deduplicate (must have String keys)
     * @param storeName the name of the state store used for deduplication
     * @param repartitionName the name of the repartition topic
     * @param windowDuration the time window during which duplicates are filtered
     * @param deduplicationHeadersList list of header names used to build the deduplication key
     * @param <V> the value type of the stream
     * @return a deduplicated stream
     */
    public static <V extends SpecificRecord> KStream<String, V> distinctByHeaders(
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