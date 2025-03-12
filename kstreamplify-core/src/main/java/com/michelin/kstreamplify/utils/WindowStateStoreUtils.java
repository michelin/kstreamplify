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
package com.michelin.kstreamplify.utils;

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.state.WindowStore;

/**
 * The window state store utils.
 *
 * @deprecated Use {@link com.michelin.kstreamplify.store.WindowStateStoreUtils}.
 */
@Deprecated(since = "1.1.0")
public class WindowStateStoreUtils {
    /**
     * Put the key/value into the state store.
     *
     * @param stateStore The stateStore
     * @param key The key
     * @param value The value
     * @param <K> The template for the key
     * @param <V> The template for the value
     */
    public static <K, V> void put(WindowStore<K, V> stateStore, K key, V value) {
        stateStore.put(key, value, Instant.now().toEpochMilli());
    }

    /**
     * Get the value by the key from the state store.
     *
     * @param stateStore The stateStore
     * @param key The key
     * @param retentionDays The delay of retention
     * @param <K> The template for the key
     * @param <V> The template for the value
     * @return The last value inserted in the state store for the key
     */
    public static <K, V> V get(WindowStore<K, V> stateStore, K key, int retentionDays) {
        var resultIterator =
                stateStore.backwardFetch(key, Instant.now().minus(Duration.ofDays(retentionDays)), Instant.now());

        if (resultIterator != null && resultIterator.hasNext()) {
            return resultIterator.next().value;
        }

        return null;
    }

    /** The private constructor. */
    private WindowStateStoreUtils() {}
}
