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

import com.michelin.kstreamplify.avro.KafkaError;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.streams.test.TestRecord;

/**
 * First-class assertions on the records sent to the dead letter queue. The assertions operate on a snapshot of all the
 * records sent to the dead letter queue since the beginning of the test.
 */
public final class DlqAssertion extends AssertionStage {
    private final String topicName;
    private final List<DlqRecord> records;

    /**
     * Constructor.
     *
     * @param context The parent test context
     * @param topicName The name of the dead letter queue topic
     * @param records The snapshot of the records sent to the dead letter queue
     */
    DlqAssertion(KstreamplifyTestContext context, String topicName, List<TestRecord<String, KafkaError>> records) {
        super(context);
        this.topicName = topicName;
        this.records = records.stream().map(DlqRecord::new).collect(Collectors.toList());
    }

    /**
     * Assert that the dead letter queue contains exactly the provided number of records.
     *
     * @param expectedCount The expected number of records
     * @return This assertion for chaining
     */
    public DlqAssertion hasExactly(int expectedCount) {
        if (records.size() != expectedCount) {
            fail(String.format(
                    "Expected %d record(s) on DLQ topic '%s' but found %d.%nActual DLQ records:%n%s",
                    expectedCount, topicName, records.size(), formatRecords()));
        }
        return this;
    }

    /**
     * Assert that no record reached the dead letter queue.
     *
     * @return This assertion for chaining
     */
    public DlqAssertion isEmpty() {
        if (!records.isEmpty()) {
            fail(String.format(
                    "Expected no record on DLQ topic '%s' but found %d.%nActual DLQ records:%n%s",
                    topicName, records.size(), formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the dead letter queue contains at least one record with the provided key.
     *
     * @param expectedKey The expected key
     * @return This assertion for chaining
     */
    public DlqAssertion containsKey(String expectedKey) {
        boolean found = records.stream().anyMatch(record -> Objects.equals(record.key(), expectedKey));
        if (!found) {
            fail(String.format(
                    "Expected a DLQ record for key '%s'.%nActual DLQ records:%n%s",
                    display(expectedKey), formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the dead letter queue contains at least one record produced by the provided exception type.
     *
     * @param exceptionType The expected exception type
     * @return This assertion for chaining
     */
    public DlqAssertion containsError(Class<? extends Throwable> exceptionType) {
        String expectedTypeName = exceptionType.getName();
        boolean found = records.stream().anyMatch(record -> expectedTypeName.equals(record.exceptionTypeName()));
        if (!found) {
            fail(String.format(
                    "Expected a DLQ record with exception type '%s'.%nActual DLQ records:%n%s",
                    expectedTypeName, formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the dead letter queue contains at least one record produced by the provided exception type and
     * carrying an error message containing the provided text.
     *
     * @param exceptionType The expected exception type
     * @param expectedMessage The expected error message fragment
     * @return This assertion for chaining
     */
    public DlqAssertion containsError(Class<? extends Throwable> exceptionType, String expectedMessage) {
        String expectedTypeName = exceptionType.getName();
        boolean found = records.stream()
                .anyMatch(record -> expectedTypeName.equals(record.exceptionTypeName())
                        && containsMessage(record, expectedMessage));
        if (!found) {
            fail(String.format(
                    "Expected a DLQ record with exception type '%s' and message containing '%s'."
                            + "%nActual DLQ records:%n%s",
                    expectedTypeName, expectedMessage, formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the dead letter queue contains at least one record whose error message contains the provided text.
     *
     * @param expectedMessage The expected error message fragment
     * @return This assertion for chaining
     */
    public DlqAssertion containsErrorMessage(String expectedMessage) {
        boolean found = records.stream().anyMatch(record -> containsMessage(record, expectedMessage));
        if (!found) {
            fail(String.format(
                    "Expected a DLQ record with an error message containing '%s'.%nActual DLQ records:%n%s",
                    expectedMessage, formatRecords()));
        }
        return this;
    }

    /**
     * Assert that the dead letter queue contains at least one record carrying the provided string header.
     *
     * @param key The expected header key
     * @param value The expected header value
     * @return This assertion for chaining
     */
    public DlqAssertion containsHeader(String key, String value) {
        boolean found = records.stream().anyMatch(record -> value.equals(record.header(key)));
        if (!found) {
            fail(String.format(
                    "Expected a DLQ record with header '%s'='%s'.%nActual '%s' headers:%n%s",
                    key,
                    value,
                    key,
                    format(records.stream()
                            .map(record -> display(record.key()) + " -> " + display(record.header(key)))
                            .collect(Collectors.toList()))));
        }
        return this;
    }

    /**
     * Assert that the dead letter queue contains at least one record satisfying the provided predicate.
     *
     * @param predicate The predicate a record must satisfy
     * @return This assertion for chaining
     */
    public DlqAssertion contains(Predicate<DlqRecord> predicate) {
        boolean found = records.stream().anyMatch(predicate);
        if (!found) {
            fail(String.format(
                    "Expected a DLQ record matching the given predicate.%nActual DLQ records:%n%s", formatRecords()));
        }
        return this;
    }

    /**
     * Apply a custom assertion to the first record sent to the dead letter queue.
     *
     * @param assertion The assertion to apply to the first DLQ record
     * @return This assertion for chaining
     */
    public DlqAssertion satisfies(Consumer<DlqRecord> assertion) {
        return satisfies(0, assertion);
    }

    /**
     * Apply a custom assertion to the record sent to the dead letter queue at the provided index.
     *
     * @param index The index of the DLQ record to assert
     * @param assertion The assertion to apply to the DLQ record
     * @return This assertion for chaining
     */
    public DlqAssertion satisfies(int index, Consumer<DlqRecord> assertion) {
        if (index >= records.size()) {
            fail(String.format(
                    "Expected at least %d record(s) on DLQ topic '%s' but found %d.%nActual DLQ records:%n%s",
                    index + 1, topicName, records.size(), formatRecords()));
        }
        assertion.accept(records.get(index));
        return this;
    }

    /**
     * Get the snapshot of the DLQ records for advanced assertions.
     *
     * @return The unmodifiable list of the DLQ records
     */
    public List<DlqRecord> records() {
        return List.copyOf(records);
    }

    /**
     * Check whether the error message of the provided record contains the provided text.
     *
     * @param record The record to inspect
     * @param expectedMessage The expected error message fragment
     * @return {@code true} if the error message contains the provided text, {@code false} otherwise
     */
    private boolean containsMessage(DlqRecord record, String expectedMessage) {
        String errorMessage = record.errorMessage();
        return errorMessage != null && errorMessage.contains(expectedMessage);
    }

    /**
     * Format the DLQ records for diagnostics.
     *
     * @return The formatted DLQ records
     */
    private String formatRecords() {
        return format(records.stream().map(DlqRecord::toString).collect(Collectors.toList()));
    }
}
