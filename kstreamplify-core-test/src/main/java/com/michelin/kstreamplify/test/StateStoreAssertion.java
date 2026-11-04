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
package com.michelin.kstreamplify.test;

import static com.michelin.kstreamplify.test.KstreamplifyTestContext.display;
import static com.michelin.kstreamplify.test.KstreamplifyTestContext.format;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Assertions on the content of a key-value state store. The assertions read the store through the underlying
 * {@link KeyValueStore} exposed by the topology test driver.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public final class StateStoreAssertion<K, V> extends AssertionStage {
    private final String storeName;
    private final KeyValueStore<K, V> store;

    /**
     * Constructor.
     *
     * @param context The parent test context
     * @param storeName The name of the state store
     * @param store The underlying key-value store
     */
    StateStoreAssertion(KstreamplifyTestContext context, String storeName, KeyValueStore<K, V> store) {
        super(context);
        this.storeName = storeName;
        this.store = store;
    }

    /**
     * Assert that the state store contains the provided key.
     *
     * @param key The expected key
     * @return This assertion for chaining
     */
    public StateStoreAssertion<K, V> containsKey(K key) {
        if (store.get(key) == null) {
            fail(String.format(
                    "Expected state store '%s' to contain key '%s'.%nActual entries:%n%s",
                    storeName, display(key), formatEntries()));
        }
        return this;
    }

    /**
     * Assert that the state store does not contain the provided key.
     *
     * @param key The key that must be absent
     * @return This assertion for chaining
     */
    public StateStoreAssertion<K, V> doesNotContainKey(K key) {
        V value = store.get(key);
        if (value != null) {
            fail(String.format(
                    "Expected state store '%s' not to contain key '%s' but it is mapped to '%s'.",
                    storeName, display(key), display(value)));
        }
        return this;
    }

    /**
     * Assert that the state store maps the provided key to the provided value.
     *
     * @param key The expected key
     * @param expectedValue The expected value
     * @return This assertion for chaining
     */
    public StateStoreAssertion<K, V> contains(K key, V expectedValue) {
        V actualValue = store.get(key);
        if (!Objects.equals(actualValue, expectedValue)) {
            fail(String.format(
                    "Expected state store '%s' to map key '%s' to '%s' but found '%s'.%nActual entries:%n%s",
                    storeName, display(key), display(expectedValue), display(actualValue), formatEntries()));
        }
        return this;
    }

    /**
     * Assert that the state store contains exactly the provided number of entries.
     *
     * @param expectedCount The expected number of entries
     * @return This assertion for chaining
     */
    public StateStoreAssertion<K, V> hasExactly(int expectedCount) {
        List<KeyValue<K, V>> entries = entries();
        if (entries.size() != expectedCount) {
            fail(String.format(
                    "Expected state store '%s' to contain %d entries but found %d.%nActual entries:%n%s",
                    storeName, expectedCount, entries.size(), formatEntries()));
        }
        return this;
    }

    /**
     * Assert that the state store is empty.
     *
     * @return This assertion for chaining
     */
    public StateStoreAssertion<K, V> isEmpty() {
        List<KeyValue<K, V>> entries = entries();
        if (!entries.isEmpty()) {
            fail(String.format(
                    "Expected state store '%s' to be empty but found %d entries.%nActual entries:%n%s",
                    storeName, entries.size(), formatEntries()));
        }
        return this;
    }

    /**
     * Assert that the state store contains at least one value satisfying the provided predicate.
     *
     * @param predicate The predicate a value must satisfy
     * @return This assertion for chaining
     */
    public StateStoreAssertion<K, V> containsValue(Predicate<V> predicate) {
        boolean found = entries().stream().map(entry -> entry.value).anyMatch(predicate);
        if (!found) {
            fail(String.format(
                    "Expected state store '%s' to contain a value matching the given predicate.%nActual entries:%n%s",
                    storeName, formatEntries()));
        }
        return this;
    }

    /**
     * Get the underlying key-value store for advanced assertions.
     *
     * @return The underlying key-value store
     */
    public KeyValueStore<K, V> store() {
        return store;
    }

    /**
     * Get all the entries of the state store.
     *
     * @return The entries of the state store
     */
    private List<KeyValue<K, V>> entries() {
        List<KeyValue<K, V>> entries = new ArrayList<>();
        try (KeyValueIterator<K, V> iterator = store.all()) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }
        return entries;
    }

    /**
     * Format the entries of the state store for diagnostics.
     *
     * @return The formatted entries
     */
    private String formatEntries() {
        return format(entries().stream()
                .map(entry -> display(entry.key) + "=" + display(entry.value))
                .collect(Collectors.toList()));
    }
}
