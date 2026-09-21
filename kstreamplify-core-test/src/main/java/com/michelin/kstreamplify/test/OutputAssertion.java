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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.test.TestRecord;

/**
 * Typed assertions on the records produced on an output topic. The assertions operate on a snapshot of all the records
 * produced on the topic since the beginning of the test.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public final class OutputAssertion<K, V> extends AssertionStage {
    private final String topicName;
    private final List<TestRecord<K, V>> records;

    /**
     * Constructor.
     *
     * @param context The parent test context
     * @param topicName The name of the output topic
     * @param records The snapshot of the records produced on the topic
     */
    OutputAssertion(KstreamplifyTestContext context, String topicName, List<TestRecord<K, V>> records) {
        super(context);
        this.topicName = topicName;
        this.records = records;
    }

    /**
     * Assert that the topic contains exactly the provided number of records.
     *
     * @param expectedCount The expected number of records
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> hasExactly(int expectedCount) {
        if (records.size() != expectedCount) {
            fail(String.format(
                    "Expected %d record(s) on topic '%s' but found %d.%nActual records:%n%s",
                    expectedCount, topicName, records.size(), formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the topic did not produce any record.
     *
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> isEmpty() {
        if (!records.isEmpty()) {
            fail(String.format(
                    "Expected no record on topic '%s' but found %d.%nActual records:%n%s",
                    topicName, records.size(), formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the topic produced at least one record with the provided key.
     *
     * @param key The expected key
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> containsKey(K key) {
        boolean found = records.stream().anyMatch(record -> Objects.equals(record.key(), key));
        if (!found) {
            fail(String.format(
                    "Expected a record with key '%s' on topic '%s'.%nActual keys:%n%s",
                    display(key), topicName, format(keys())));
        }
        return this;
    }

    /**
     * Assert that the topic did not produce any record with the provided key.
     *
     * @param key The key that must be absent
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> doesNotContainKey(K key) {
        boolean found = records.stream().anyMatch(record -> Objects.equals(record.key(), key));
        if (found) {
            fail(String.format(
                    "Expected no record with key '%s' on topic '%s'.%nActual records:%n%s",
                    display(key), topicName, formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the topic produced at least one record matching the provided key and value.
     *
     * @param key The expected key
     * @param value The expected value
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> containsRecord(K key, V value) {
        boolean found = records.stream()
                .anyMatch(record -> Objects.equals(record.key(), key) && Objects.equals(record.value(), value));
        if (!found) {
            fail(String.format(
                    "Expected a record with key '%s' and value '%s' on topic '%s'.%nActual records:%n%s",
                    display(key), display(value), topicName, formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the topic produced at least one record whose value satisfies the provided predicate.
     *
     * @param predicate The predicate the value must satisfy
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> containsValue(Predicate<V> predicate) {
        boolean found = records.stream().map(TestRecord::value).anyMatch(predicate);
        if (!found) {
            fail(String.format(
                    "Expected a record matching the given value predicate on topic '%s'.%nActual records:%n%s",
                    topicName, formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the topic produced at least one record carrying the provided string header.
     *
     * @param key The expected header key
     * @param value The expected header value
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> containsHeader(String key, String value) {
        boolean found = records.stream().anyMatch(record -> value.equals(header(record, key)));
        if (!found) {
            fail(String.format(
                    "Expected a record with header '%s'='%s' on topic '%s'.%nActual '%s' headers:%n%s",
                    key, value, topicName, key, format(headers(key))));
        }
        return this;
    }

    /**
     * Assert that the records produced on the topic match exactly the provided key-value entries, in order.
     *
     * @param entries The expected key-value entries
     * @return This assertion for chaining
     */
    @SafeVarargs
    public final OutputAssertion<K, V> containsExactly(Map.Entry<K, V>... entries) {
        List<Map.Entry<K, V>> expectedEntries = List.of(entries);
        if (records.size() != expectedEntries.size()) {
            fail(String.format(
                    "Expected %d record(s) on topic '%s' but found %d.%nMissing keys:%n%s%nUnexpected keys:%n%s"
                            + "%nActual records:%n%s",
                    expectedEntries.size(),
                    topicName,
                    records.size(),
                    format(missingKeys(expectedEntries)),
                    format(unexpectedKeys(expectedEntries)),
                    formatRecords()));
        }

        for (int index = 0; index < expectedEntries.size(); index++) {
            TestRecord<K, V> actual = records.get(index);
            Map.Entry<K, V> expected = expectedEntries.get(index);
            if (!Objects.equals(actual.key(), expected.getKey())
                    || !Objects.equals(actual.value(), expected.getValue())) {
                fail(String.format(
                        "Record at index %d on topic '%s' does not match.%nExpected: %s=%s%nActual:   %s=%s",
                        index,
                        topicName,
                        display(expected.getKey()),
                        display(expected.getValue()),
                        display(actual.key()),
                        display(actual.value())));
            }
        }
        return this;
    }

    /**
     * Apply a custom assertion to the first record produced on the topic.
     *
     * @param assertion The assertion to apply to the first record
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> satisfies(Consumer<TestRecord<K, V>> assertion) {
        return satisfies(0, assertion);
    }

    /**
     * Apply a custom assertion to the record produced at the provided index.
     *
     * @param index The index of the record to assert
     * @param assertion The assertion to apply to the record
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> satisfies(int index, Consumer<TestRecord<K, V>> assertion) {
        if (index >= records.size()) {
            fail(String.format(
                    "Expected at least %d record(s) on topic '%s' but found %d.%nActual records:%n%s",
                    index + 1, topicName, records.size(), formatRecords()));
        }
        assertion.accept(records.get(index));
        return this;
    }

    /**
     * Apply a custom assertion to every record produced on the topic. It fails when the topic did not produce any
     * record.
     *
     * @param assertion The assertion to apply to each record
     * @return This assertion for chaining
     */
    public OutputAssertion<K, V> allSatisfy(Consumer<TestRecord<K, V>> assertion) {
        if (records.isEmpty()) {
            fail(String.format("Expected at least one record on topic '%s' but found none.", topicName));
        }
        records.forEach(assertion);
        return this;
    }

    /**
     * Get the snapshot of the records produced on the topic for advanced assertions.
     *
     * @return The unmodifiable list of the produced records
     */
    public List<TestRecord<K, V>> records() {
        return records;
    }

    /**
     * Get the value of the provided header of a record.
     *
     * @param record The record to inspect
     * @param key The header key
     * @return The header value, or {@code null} if the header is absent
     */
    private String header(TestRecord<K, V> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * Get the keys of the records produced on the topic.
     *
     * @return The formatted keys
     */
    private List<String> keys() {
        return records.stream().map(record -> display(record.key())).collect(Collectors.toList());
    }

    /**
     * Get the values of the provided header for all the records produced on the topic.
     *
     * @param key The header key
     * @return The formatted header values
     */
    private List<String> headers(String key) {
        return records.stream()
                .map(record -> display(record.key()) + " -> " + display(header(record, key)))
                .collect(Collectors.toList());
    }

    /**
     * Get the expected keys that were not produced on the topic.
     *
     * @param expectedEntries The expected key-value entries
     * @return The formatted missing keys
     */
    private List<String> missingKeys(List<Map.Entry<K, V>> expectedEntries) {
        List<String> actualKeys = keys();
        List<String> missingKeys = new ArrayList<>();
        expectedEntries.stream()
                .map(entry -> display(entry.getKey()))
                .filter(key -> !actualKeys.contains(key))
                .forEach(missingKeys::add);
        return missingKeys;
    }

    /**
     * Get the keys produced on the topic that were not expected.
     *
     * @param expectedEntries The expected key-value entries
     * @return The formatted unexpected keys
     */
    private List<String> unexpectedKeys(List<Map.Entry<K, V>> expectedEntries) {
        List<String> expectedKeys =
                expectedEntries.stream().map(entry -> display(entry.getKey())).toList();
        return keys().stream().filter(key -> !expectedKeys.contains(key)).toList();
    }

    /**
     * Format the records produced on the topic for diagnostics.
     *
     * @return The formatted records
     */
    private String formatRecords() {
        return format(records.stream()
                .map(record -> display(record.key()) + "=" + display(record.value()))
                .collect(Collectors.toList()));
    }
}
