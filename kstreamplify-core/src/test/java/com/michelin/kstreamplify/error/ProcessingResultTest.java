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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

class ProcessingResultTest {

    @Test
    void shouldCreateProcessingResultSuccess() {
        String successValue = "Success";
        ProcessingResult<String, Integer> result = ProcessingResult.success(successValue);

        assertTrue(result.isValid());
        assertEquals(successValue, result.getValue());
        assertNull(result.getError());
    }

    @Test
    void shouldCreateWrappedProcessingResultFromRecord() {
        Record<String, String> message = new Record<>("key", "value", System.currentTimeMillis());
        Record<String, ProcessingResult<String, Integer>> wrappedRecord = ProcessingResult.wrapRecordSuccess(message);

        assertTrue(wrappedRecord.value().isValid());
        assertEquals(message.key(), wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertEquals(message.value(), wrappedRecord.value().getValue());
        assertEquals(message.timestamp(), wrappedRecord.timestamp());
        assertNull(wrappedRecord.value().getError());
    }

    @Test
    void shouldCreateWrappedProcessingResultFromParameters() {
        String key = "key";
        String value = "value";
        long timestamp = System.currentTimeMillis();

        Record<String, ProcessingResult<String, Integer>> wrappedRecord =
                ProcessingResult.wrapRecordSuccess(key, value, timestamp);

        assertTrue(wrappedRecord.value().isValid());
        assertEquals(key, wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertEquals(value, wrappedRecord.value().getValue());
        assertEquals(timestamp, wrappedRecord.timestamp());
        assertNull(wrappedRecord.value().getError());
    }

    @Test
    void shouldWrapRecordSuccessWithHeadersFromRecord() {
        String headerKey = "header_key";
        String headerValue = "header_value";

        Headers headers = new RecordHeaders(
                Collections.singletonList(new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));

        Record<String, String> message = new Record<>("key", "value", System.currentTimeMillis(), headers);
        Record<String, ProcessingResult<String, Integer>> wrappedRecord =
                ProcessingResult.wrapRecordSuccessWithHeaders(message);

        assertTrue(wrappedRecord.value().isValid());
        assertEquals(message.key(), wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertEquals(message.value(), wrappedRecord.value().getValue());
        assertEquals(message.timestamp(), wrappedRecord.timestamp());
        assertEquals(1, wrappedRecord.headers().toArray().length);
        assertEquals(
                message.headers().lastHeader(headerKey).value(),
                wrappedRecord.headers().lastHeader(headerKey).value());
        assertNull(wrappedRecord.value().getError());
    }

    @Test
    void shouldWrapRecordSuccessWithHeadersFromParameters() {
        String key = "key";
        String value = "value";
        String headerKey = "header_key";
        String headerValue = "header_value";
        long timestamp = System.currentTimeMillis();

        Headers headers = new RecordHeaders(
                Collections.singletonList(new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));

        Record<String, ProcessingResult<String, Integer>> wrappedRecord =
                ProcessingResult.wrapRecordSuccess(key, value, timestamp, headers);

        assertTrue(wrappedRecord.value().isValid());
        assertEquals(key, wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertEquals(value, wrappedRecord.value().getValue());
        assertEquals(timestamp, wrappedRecord.timestamp());
        assertEquals(1, wrappedRecord.headers().toArray().length);
        assertEquals(
                headers.lastHeader(headerKey).value(),
                wrappedRecord.headers().lastHeader(headerKey).value());
        assertNull(wrappedRecord.value().getError());
    }

    @Test
    void shouldCreateFailedProcessingResult() {
        String failedRecordValue = "value";
        Exception exception = new Exception("Exception");

        ProcessingResult<String, String> result = ProcessingResult.fail(exception, failedRecordValue);

        assertFalse(result.isValid());
        assertNotNull(result.getError());
        assertEquals(exception, result.getError().getException());
        assertEquals(failedRecordValue, result.getError().getKafkaRecord());
        assertEquals("No context message", result.getError().getContextMessage());
        assertNull(result.getValue());
    }

    @Test
    void shouldCreateFailedProcessingResultWithContextMessage() {
        String failedRecordValue = "value";
        Exception exception = new Exception("Exception");
        String contextMessage = "Context message";

        ProcessingResult<String, String> result = ProcessingResult.fail(exception, failedRecordValue, contextMessage);

        assertFalse(result.isValid());
        assertNotNull(result.getError());
        assertEquals(exception, result.getError().getException());
        assertEquals(failedRecordValue, result.getError().getKafkaRecord());
        assertEquals(contextMessage, result.getError().getContextMessage());
        assertNull(result.getValue());
    }

    @Test
    void shouldCreateWrappedFailedProcessingResultFromRecord() {
        Exception exception = new Exception("Exception");

        Record<String, String> message = new Record<>("key", "value", System.currentTimeMillis());
        Record<String, ProcessingResult<String, String>> wrappedRecord =
                ProcessingResult.wrapRecordFailure(exception, message);

        assertEquals(message.key(), wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertFalse(wrappedRecord.value().isValid());
        assertNull(wrappedRecord.value().getValue());
        assertNotNull(wrappedRecord.value().getError());
        assertEquals(exception, wrappedRecord.value().getError().getException());
        assertEquals(message.value(), wrappedRecord.value().getError().getKafkaRecord());
        assertEquals("No context message", wrappedRecord.value().getError().getContextMessage());
        assertEquals(message.timestamp(), wrappedRecord.timestamp());
    }

    @Test
    void shouldCreateWrappedFailedProcessingResultWithContextMessageFromRecord() {
        Exception exception = new Exception("Exception");
        String contextMessage = "Context message";

        Record<String, String> message = new Record<>("key", "value", System.currentTimeMillis());
        Record<String, ProcessingResult<String, String>> wrappedRecord =
                ProcessingResult.wrapRecordFailure(exception, message, contextMessage);

        assertEquals(message.key(), wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertFalse(wrappedRecord.value().isValid());
        assertNull(wrappedRecord.value().getValue());
        assertNotNull(wrappedRecord.value().getError());
        assertEquals(exception, wrappedRecord.value().getError().getException());
        assertEquals(message.value(), wrappedRecord.value().getError().getKafkaRecord());
        assertEquals(contextMessage, wrappedRecord.value().getError().getContextMessage());
        assertEquals(message.timestamp(), wrappedRecord.timestamp());
    }

    @Test
    void shouldCreateWrappedFailedProcessingResultFromParameters() {
        String key = "key";
        String value = "value";
        long timestamp = System.currentTimeMillis();
        Exception exception = new Exception("Exception");

        Record<String, ProcessingResult<String, String>> wrappedRecord =
                ProcessingResult.wrapRecordFailure(exception, key, value, timestamp);

        assertEquals(key, wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertFalse(wrappedRecord.value().isValid());
        assertNull(wrappedRecord.value().getValue());
        assertNotNull(wrappedRecord.value().getError());
        assertEquals(exception, wrappedRecord.value().getError().getException());
        assertEquals(value, wrappedRecord.value().getError().getKafkaRecord());
        assertEquals("No context message", wrappedRecord.value().getError().getContextMessage());
        assertEquals(timestamp, wrappedRecord.timestamp());
    }

    @Test
    void shouldCreateWrappedFailedProcessingResultFromParametersWithContextMessage() {
        String key = "key";
        String value = "value";
        long timestamp = System.currentTimeMillis();
        Exception exception = new Exception("Exception");
        String contextMessage = "Context message";

        Record<String, ProcessingResult<String, String>> wrappedRecord =
                ProcessingResult.wrapRecordFailure(exception, key, value, timestamp, contextMessage);

        assertEquals(key, wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertFalse(wrappedRecord.value().isValid());
        assertNull(wrappedRecord.value().getValue());
        assertNotNull(wrappedRecord.value().getError());
        assertEquals(exception, wrappedRecord.value().getError().getException());
        assertEquals(value, wrappedRecord.value().getError().getKafkaRecord());
        assertEquals(contextMessage, wrappedRecord.value().getError().getContextMessage());
        assertEquals(timestamp, wrappedRecord.timestamp());
    }

    @Test
    void shouldProcessingResultBeValid() {
        ProcessingResult<String, Integer> validResult = ProcessingResult.success("Value");
        ProcessingResult<String, Integer> invalidResult1 = ProcessingResult.fail(new Exception(), 42);

        assertTrue(validResult.isValid());
        assertFalse(invalidResult1.isValid());
    }
}
