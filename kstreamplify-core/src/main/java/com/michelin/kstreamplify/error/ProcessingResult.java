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
 * @param <V>  The type of the successful record
 * @param <V2> The type of the failed record
 */
@Getter
public class ProcessingResult<V, V2> {
    /**
     * The successful record.
     */
    private V value;

    /**
     * The failed record wrapped in a processing error.
     */
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
     * Create a successful processing result.
     *
     * @param value The successful record value
     * @param <V>   The type of the successful record
     * @param <V2>  The type of the failed record
     * @return A processing result containing a successful record
     */
    public static <V, V2> ProcessingResult<V, V2> success(V value) {
        return new ProcessingResult<>(value);
    }

    /**
     * Wraps a record's value with ProcessingResult.success(V value).
     * The resulting stream needs to be handled with TopologyErrorHandler#catchErrors(KStream)
     * for automatic DLQ redirection on failed records.
     *
     * @param message The resulting successful Record from the processor that needs to be wrapped in a ProcessingResult
     * @param <K>     The type of the record key
     * @param <V>     The type of the ProcessingResult successful value
     * @param <V2>    The type of the ProcessingResult error value
     * @return The initial Record, with value wrapped in a ProcessingResult
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordSuccess(Record<K, V> message) {
        return new Record<>(
            message.key(),
            ProcessingResult.success(message.value()),
            message.timestamp()
        );
    }

    /**
     * Wraps a key, value and timestamp in a Record with ProcessingResult#success(V value) as value.
     * The resulting stream needs to be handled with TopologyErrorHandler#catchErrors(KStream)
     * for automatic DLQ redirection on failed records.
     *
     * @param key       The key to put in the resulting record
     * @param value     The successful value to put in the resulting record
     * @param timestamp The timestamp to apply on the resulting record
     * @param <K>       The type of the record key
     * @param <V>       The type of the ProcessingResult successful value
     * @param <V2>      The type of the ProcessingResult error value
     * @return A Record with value wrapped in a {@link ProcessingResult}
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordSuccess(K key, V value, long timestamp) {
        return new Record<>(key, ProcessingResult.success(value), timestamp);
    }

    /**
     * Wraps a key, value, timestamp and headers in a Record with ProcessingResult#success(V value) as value.
     * The resulting stream needs to be handled with TopologyErrorHandler#catchErrors(KStream)
     * for automatic DLQ redirection on failed records.
     *
     * @param key       The key to put in the resulting record
     * @param value     The successful value to put in the resulting record
     * @param timestamp The timestamp to apply on the resulting record
     * @param headers    The headers values to put in the resulting record
     * @param <K>       The type of the record key
     * @param <V>       The type of the ProcessingResult successful value
     * @param <V2>      The type of the ProcessingResult error value
     * @return A Record with value wrapped in a {@link ProcessingResult}
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordSuccess(K key, 
                                                                                  V value, 
                                                                                  long timestamp, 
                                                                                  Headers headers) {
        return new Record<>(key, ProcessingResult.success(value), timestamp, headers);
    }

    /**
     * Wraps a record's value and the headers with ProcessingResult.success(V value).
     * The resulting stream needs to be handled with TopologyErrorHandler#catchErrors(KStream)
     * for automatic DLQ redirection on failed records.
     *
     * @param message The resulting successful Record from the processor that needs to be wrapped in a ProcessingResult
     * @param <K>     The type of the record key
     * @param <V>     The type of the ProcessingResult successful value
     * @param <V2>    The type of the ProcessingResult error value
     * @return The initial Record, with value wrapped in a ProcessingResult
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordSuccessWithHeaders(Record<K, V> message) {
        return new Record<>(
            message.key(),
            ProcessingResult.success(message.value()),
            message.timestamp(),
            message.headers()
        );
    }

    /**
     * Create a failed processing result.
     * If you are using this in a Processor, refer to
     * {@link ProcessingResult#wrapRecordFailure(Exception, Record)} for easier syntax.
     *
     * @param e                 The exception
     * @param failedRecordValue The failed Kafka record
     * @param <V>               The type of the successful record
     * @param <V2>              The type of the failed record
     * @return A processing result containing the failed record
     */
    public static <V, V2> ProcessingResult<V, V2> fail(Exception e, V2 failedRecordValue) {
        return new ProcessingResult<>(new ProcessingError<>(e, failedRecordValue));
    }

    /**
     * Create a failed processing result.
     * If you are using this in a Processor, refer to
     * {@link ProcessingResult#wrapRecordFailure(Exception, Record, String)}
     * for easier syntax.
     *
     * @param e                 The exception
     * @param failedRecordValue The failed Kafka record
     * @param contextMessage    The custom context message that will be added in the stack trace
     * @param <V>               The type of the successful record
     * @param <V2>              The type of the failed record
     * @return A processing result containing the failed record
     */
    public static <V, V2> ProcessingResult<V, V2> fail(Exception e, V2 failedRecordValue, String contextMessage) {
        return new ProcessingResult<>(new ProcessingError<>(e, contextMessage, failedRecordValue));
    }

    /**
     * Wraps a record's value with {@link ProcessingResult#fail(Exception, Object)} )}.
     * The resulting stream needs to be handled with TopologyErrorHandler#catchErrors(KStream) for automatic
     * DLQ redirection on failed records.
     *
     * @param e            The initial exception
     * @param failedRecord The resulting failed Record from
     *                     the processor that needs to be wrapped in a {@link ProcessingResult}
     * @param <K>          The type of the record key
     * @param <V>          The type of the ProcessingResult successful value
     * @param <V2>         The type of the ProcessingResult error value
     * @return The initial Record, with value wrapped in a {@link ProcessingError} and {@link ProcessingResult}
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordFailure(Exception e,
                                                                                  Record<K, V2> failedRecord) {
        return new Record<>(
            failedRecord.key(),
            ProcessingResult.fail(e, failedRecord.value()),
            failedRecord.timestamp()
        );
    }

    /**
     * Wraps a record's value with {@link ProcessingResult#fail(Exception, Object, String)}.
     * The resulting stream needs to be handled with TopologyErrorHandler#catchErrors(KStream)
     * for automatic DLQ redirection on failed records.
     *
     * @param e              The initial exception
     * @param failedRecord   The resulting failed Record from
     *                       the processor that needs to be wrapped in a {@link ProcessingResult}
     * @param contextMessage The custom context message that will be added in the stack trace
     * @param <K>            The type of the record key
     * @param <V>            The type of the ProcessingResult successful value
     * @param <V2>           The type of the ProcessingResult error value
     * @return The initial Record, with value wrapped in a {@link ProcessingError} and {@link ProcessingResult}
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordFailure(Exception e,
                                                                                  Record<K, V2> failedRecord,
                                                                                  String contextMessage) {
        return new Record<>(
            failedRecord.key(),
            ProcessingResult.fail(e, failedRecord.value(), contextMessage),
            failedRecord.timestamp()
        );
    }

    /**
     * Wraps a key, value and timestamp in a Record with {@link ProcessingResult#fail(Exception, Object, String)}
     * as value.
     * The resulting stream needs to be handled with TopologyErrorHandler#catchErrors(KStream) for automatic
     * DLQ redirection on failed records.
     *
     * @param e           The initial exception
     * @param key         The key to put in the resulting record
     * @param failedValue The resulting failed value from
     *                    the processor that needs to be wrapped in a {@link ProcessingResult}
     * @param timestamp   The timestamp to apply on the resulting record
     * @param <K>         The type of the record key
     * @param <V>         The type of the ProcessingResult successful value
     * @param <V2>        The type of the ProcessingResult error value
     * @return A Record with value wrapped in a {@link ProcessingError} and {@link ProcessingResult}
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordFailure(Exception e,
                                                                                  K key,
                                                                                  V2 failedValue,
                                                                                  long timestamp) {
        return new Record<>(key, ProcessingResult.fail(e, failedValue), timestamp);
    }

    /**
     * Wraps a key, value and timestamp in a Record
     * with {@link ProcessingResult#fail(Exception, Object, String)} as value.
     * The resulting stream needs to be handled with TopologyErrorHandler#catchErrors(KStream) for automatic
     * DLQ redirection on failed records.
     *
     * @param e              The initial exception
     * @param key            The key to put in the resulting record
     * @param failedValue    The resulting failed value from the processor
     *                       that needs to be wrapped in a {@link ProcessingResult}
     * @param timestamp      The timestamp to apply on the resulting record
     * @param contextMessage The custom context message that will be added in the stack trace
     * @param <K>            The type of the record key
     * @param <V>            The type of the ProcessingResult successful value
     * @param <V2>           The type of the ProcessingResult error value
     * @return A Record with value wrapped in a {@link ProcessingError} and {@link ProcessingResult}
     */
    public static <K, V, V2> Record<K, ProcessingResult<V, V2>> wrapRecordFailure(Exception e,
                                                                                  K key,
                                                                                  V2 failedValue,
                                                                                  long timestamp,
                                                                                  String contextMessage) {
        return new Record<>(key, ProcessingResult.fail(e, failedValue, contextMessage), timestamp);
    }

    /**
     * Is the processing result valid.
     * Is it valid either if it contains a successful value or an error
     *
     * @return true if valid, false otherwise
     */
    public boolean isValid() {
        return value != null && error == null;
    }
}
