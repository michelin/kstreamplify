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
package com.michelin.kstreamplify.store;

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.state.WindowStore;

/** The window state store utils. */
public final class WindowStateStoreUtils {
    /** Private constructor. */
    private WindowStateStoreUtils() {}

    /**
     * Puts a key/value pair into the {@link WindowStore} using the current system time as the record timestamp.
     *
     * @param stateStore The target state store
     * @param key The record key
     * @param value The record value
     * @param <K> The key type
     * @param <V> The value type
     */
    public static <K, V> void put(WindowStore<K, V> stateStore, K key, V value) {
        put(stateStore, key, value, Instant.now().toEpochMilli());
    }

    /**
     * Puts a key/value pair into the {@link WindowStore} using the provided timestamp.
     *
     * @param stateStore The target state store
     * @param key The record key
     * @param value The record value
     * @param timestamp The timestamp associated with the record (epoch milliseconds)
     * @param <K> The key type
     * @param <V> The value type
     */
    public static <K, V> void put(WindowStore<K, V> stateStore, K key, V value, long timestamp) {
        stateStore.put(key, value, timestamp);
    }

    /**
     * Gets the latest value associated with the given key from the {@link WindowStore} within the specified retention
     * period.
     *
     * @param stateStore The source state store
     * @param key The record key
     * @param retentionDays The retention period in days to look back from the current time
     * @param <K> The key type
     * @param <V> The value type
     * @return The most recent value for the key within the retention window, or {@code null} if none exists
     */
    public static <K, V> V get(WindowStore<K, V> stateStore, K key, int retentionDays) {
        Instant now = Instant.now();
        return get(stateStore, key, now.minus(Duration.ofDays(retentionDays)), now);
    }

    /**
     * Gets the latest value associated with the given key from the {@link WindowStore} within the provided time range.
     *
     * @param stateStore The source state store
     * @param key The record key
     * @param from The start timestamp (inclusive)
     * @param to The end timestamp (inclusive)
     * @param <K> The key type
     * @param <V> The value type
     * @return The most recent value for the key within the given time range, or {@code null} if none exists
     */
    public static <K, V> V get(WindowStore<K, V> stateStore, K key, Instant from, Instant to) {
        var it = stateStore.backwardFetch(key, from, to);
        if (it == null) {
            return null;
        }
        try (it) {
            return it.hasNext() ? it.next().value : null;
        }
    }
}
