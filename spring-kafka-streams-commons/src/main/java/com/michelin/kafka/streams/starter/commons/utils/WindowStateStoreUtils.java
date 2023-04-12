package com.michelin.kafka.streams.starter.commons.utils;

import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.Instant;

public class WindowStateStoreUtils {
    private WindowStateStoreUtils() { }

    /**
     * Put the key/value into the state store
     * @param stateStore The stateStore
     * @param key        The key
     * @param value      The value
     * @param <K>        The template for the key
     * @param <V>        The template for the value
     */
    public static <K, V> void put(WindowStore<K, V> stateStore, K key, V value) {
        stateStore.put(key, value, Instant.now().toEpochMilli());
    }

    /**
     * Get the value by the key from the stateStore
     *
     * @param stateStore    The stateStore
     * @param key           The key
     * @param retentionDays The delay of retention
     * @param <K>           The template for the key
     * @param <V>           The template for the value
     * @return The last value inserted in the stateStore for the key
     */
    public static <K, V> V get(WindowStore<K, V> stateStore, K key, int retentionDays) {
        var resultIterator = stateStore.backwardFetch(key, Instant.now().minus(Duration.ofDays(retentionDays)), Instant.now());
        if (resultIterator != null && resultIterator.hasNext()) {
            return resultIterator.next().value;
        }
        return null;
    }
}
