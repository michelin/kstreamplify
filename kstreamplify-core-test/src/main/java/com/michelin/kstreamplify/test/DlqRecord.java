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

import com.michelin.kstreamplify.avro.KafkaError;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.test.TestRecord;

/**
 * Immutable view of a record that reached the dead letter queue. It hides the internal {@link KafkaError}
 * representation behind a behavior-oriented API while still exposing the raw error for advanced assertions.
 */
public final class DlqRecord {
    private final String key;
    private final KafkaError error;
    private final Headers headers;

    /**
     * Constructor.
     *
     * @param record The raw record read from the dead letter queue
     */
    DlqRecord(TestRecord<String, KafkaError> record) {
        this.key = record.key();
        this.error = record.value();
        this.headers = record.headers() != null ? record.headers() : new RecordHeaders();
    }

    /**
     * Get the key of the failed record.
     *
     * @return The key of the failed record
     */
    public String key() {
        return key;
    }

    /**
     * Get the raw error stored in the dead letter queue.
     *
     * @return The raw error
     */
    public KafkaError error() {
        return error;
    }

    /**
     * Get the headers of the record sent to the dead letter queue.
     *
     * @return The headers of the record
     */
    public Headers headers() {
        return headers;
    }

    /**
     * Get the value of the provided string header.
     *
     * @param key The header key
     * @return The header value, or {@code null} if the header is absent
     */
    public String header(String key) {
        Header header = headers.lastHeader(key);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * Get the fully qualified name of the exception, parsed from the stack trace.
     *
     * @return The exception type name, or {@code null} if it cannot be resolved
     */
    public String exceptionTypeName() {
        if (error == null || error.getStack() == null || error.getStack().isBlank()) {
            return null;
        }

        String firstLine = error.getStack().lines().findFirst().orElse("");
        int colonIndex = firstLine.indexOf(':');
        String exceptionTypeName = colonIndex >= 0 ? firstLine.substring(0, colonIndex) : firstLine;
        return exceptionTypeName.trim();
    }

    /**
     * Get the error message of the exception that caused the failure.
     *
     * @return The error message, or {@code null} if none
     */
    public String errorMessage() {
        if (error == null) {
            return null;
        }
        return error.getCause();
    }

    /**
     * Get the context message attached to the failure.
     *
     * @return The context message, or {@code null} if none
     */
    public String contextMessage() {
        if (error == null) {
            return null;
        }
        return error.getContextMessage();
    }

    /**
     * Format this record for diagnostics.
     *
     * @return The formatted record
     */
    @Override
    public String toString() {
        return String.format(
                "key='%s', exception=%s, message=%s",
                KstreamplifyTestContext.display(key),
                KstreamplifyTestContext.display(exceptionTypeName()),
                KstreamplifyTestContext.display(errorMessage()));
    }
}
