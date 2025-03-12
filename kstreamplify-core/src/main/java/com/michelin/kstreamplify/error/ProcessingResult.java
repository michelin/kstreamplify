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
package com.michelin.kstreamplify.error;

import lombok.Getter;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Record;

/**
 * The processing result class.
 *
 * @param <V> The type of the successful record
 * @param <V2> The type of the failed record
 */
@Getter
public class ProcessingResult<V, V2> {
    /** The successful record. */
    private V value;

    /** The failed record wrapped in a processing error. */
    private ProcessingError<V2> error;

    /**
     * Private constructor that sets the success value.
     *
     * @param value The success value
     */
    private ProcessingResult(V value) {
        this.value = value;
    }

    /**
     * Private constructor that sets the error value.
     *
     * @param error the ProcessingError containing the
     */
    private ProcessingResult(ProcessingError<V2> error) {
        this.error = error;
    }

    /**
     * Create a successful {@link ProcessingResult}.
     *
     * @param value The successful record value
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     * @return A {@link ProcessingResult} containing a successful value
     */
    public static <V, V2> ProcessingResult<V, V2> success(V value) {
        return new ProcessingResult<>(value);
    }

    /**
     * Create a {@link Record} with a successful {@link ProcessingResult}.
     *
     * @param message The successful record
     * @param <K> The type of the record key
     * @param <V> The type of the ProcessingResult successful value
     * @param <V2> The type of the ProcessingResult error value
     * @return A {@link Record} with a {@link ProcessingResult} containing a successful value
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordSuccess(Record<K, V> message) {
        return new Record<>(message.key(), ProcessingResult.success(message.value()), message.timestamp());
    }

    /**
     * Create a {@link Record} with a successful {@link ProcessingResult}.
     *
     * @param key The key to put in the resulting record
     * @param value The successful value to put in the resulting record
     * @param timestamp The timestamp to apply on the resulting record
     * @param <K> The type of the record key
     * @param <V> The type of the ProcessingResult successful value
     * @param <V2> The type of the ProcessingResult error value
     * @return A {@link Record} with a {@link ProcessingResult} containing a successful value
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordSuccess(K key, V value, long timestamp) {
        return new Record<>(key, ProcessingResult.success(value), timestamp);
    }

    /**
     * Create a {@link Record} with headers and a successful {@link ProcessingResult}.
     *
     * @param key The key to put in the resulting record
     * @param value The successful value to put in the resulting record
     * @param timestamp The timestamp to apply on the resulting record
     * @param headers The headers values to put in the resulting record
     * @param <K> The type of the record key
     * @param <V> The type of the ProcessingResult successful value
     * @param <V2> The type of the ProcessingResult error value
     * @return A {@link Record} with a {@link ProcessingResult} containing a successful value
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordSuccess(
            K key, V value, long timestamp, Headers headers) {
        return new Record<>(key, ProcessingResult.success(value), timestamp, headers);
    }

    /**
     * Create a {@link Record} with headers and a successful {@link ProcessingResult}.
     *
     * @param message The successful record
     * @param <K> The type of the record key
     * @param <V> The type of the ProcessingResult successful value
     * @param <V2> The type of the ProcessingResult error value
     * @return A {@link Record} with a {@link ProcessingResult} containing a successful value
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordSuccessWithHeaders(Record<K, V> message) {
        return new Record<>(
                message.key(), ProcessingResult.success(message.value()), message.timestamp(), message.headers());
    }

    /**
     * Create a failed {@link ProcessingResult}.
     *
     * @param exception The exception
     * @param value The failed record value
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     * @return A {@link ProcessingResult} containing a failed value
     */
    public static <V, V2> ProcessingResult<V, V2> fail(Exception exception, V2 value) {
        return new ProcessingResult<>(new ProcessingError<>(exception, value));
    }

    /**
     * Create a failed {@link ProcessingResult} with a custom context message.
     *
     * @param exception The exception
     * @param value The failed record value
     * @param contextMessage The custom context message
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     * @return A {@link ProcessingResult} containing a failed value
     */
    public static <V, V2> ProcessingResult<V, V2> fail(Exception exception, V2 value, String contextMessage) {
        return new ProcessingResult<>(new ProcessingError<>(exception, contextMessage, value));
    }

    /**
     * Create a {@link Record} with a failed {@link ProcessingResult}.
     *
     * @param exception The initial exception
     * @param message The failed record
     * @param <K> The type of the record key
     * @param <V> The type of the ProcessingResult successful value
     * @param <V2> The type of the ProcessingResult error value
     * @return A {@link Record} with a {@link ProcessingResult} containing a failed value
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordFailure(
            Exception exception, Record<K, V2> message) {
        return new Record<>(message.key(), ProcessingResult.fail(exception, message.value()), message.timestamp());
    }

    /**
     * Create a {@link Record} with a failed {@link ProcessingResult} with a custom context message.
     *
     * @param exception The initial exception
     * @param message The failed record
     * @param contextMessage The custom context message that will be added in the stack trace
     * @param <K> The type of the record key
     * @param <V> The type of the ProcessingResult successful value
     * @param <V2> The type of the ProcessingResult error value
     * @return A {@link Record} with a {@link ProcessingResult} containing a failed value
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordFailure(
            Exception exception, Record<K, V2> message, String contextMessage) {
        return new Record<>(
                message.key(), ProcessingResult.fail(exception, message.value(), contextMessage), message.timestamp());
    }

    /**
     * Create a {@link Record} with a failed {@link ProcessingResult}.
     *
     * @param exception The initial exception
     * @param key The key to put in the resulting record
     * @param value The failed record value
     * @param timestamp The timestamp to apply on the resulting record
     * @param <K> The type of the record key
     * @param <V> The type of the ProcessingResult successful value
     * @param <V2> The type of the ProcessingResult error value
     * @return A {@link Record} with a {@link ProcessingResult} containing a failed value
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordFailure(
            Exception exception, K key, V2 value, long timestamp) {
        return new Record<>(key, ProcessingResult.fail(exception, value), timestamp);
    }

    /**
     * Create a {@link Record} with a failed {@link ProcessingResult} with a custom context message.
     *
     * @param exception The initial exception
     * @param key The key to put in the resulting record
     * @param value The failed record value
     * @param timestamp The timestamp to apply on the resulting record
     * @param contextMessage The custom context message that will be added in the stack trace
     * @param <K> The type of the record key
     * @param <V> The type of the ProcessingResult successful value
     * @param <V2> The type of the ProcessingResult error value
     * @return A {@link Record} with a {@link ProcessingResult} containing a failed value
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordFailure(
            Exception exception, K key, V2 value, long timestamp, String contextMessage) {
        return new Record<>(key, ProcessingResult.fail(exception, value, contextMessage), timestamp);
    }

    /**
     * Is the processing result valid. Is it valid either if it contains a successful value or an error
     *
     * @return true if valid, false otherwise
     */
    public boolean isValid() {
        return value != null && error == null;
    }
}
