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
    void shouldCreateWrappedProcessingResult() {
        String value = "Value";
        long timestamp = System.currentTimeMillis();

        Record<String, String> message = new Record<>("key", value, timestamp);
        Record<String, ProcessingResult<String, Integer>> wrappedRecord = ProcessingResult.wrapRecordSuccess(message);

        assertEquals(message.key(), wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertTrue(wrappedRecord.value().isValid());
        assertEquals(value, wrappedRecord.value().getValue());
        assertNull(wrappedRecord.value().getError());
        assertEquals(message.timestamp(), wrappedRecord.timestamp());
    }

    @Test
    void shouldCreateWrappedProcessingResultWithHeader() {
        String value = "Value";
        String headerKey = "MSG_HEADER";
        String headerValue = "Header value";
        long timestamp = System.currentTimeMillis();

        Headers headers = new RecordHeaders(Collections.singletonList(
                new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));
        
        Record<String, String> message = new Record<>("key", value, timestamp, headers);
        Record<String, ProcessingResult<String, Integer>> wrappedRecord = 
                                                            ProcessingResult.wrapRecordSuccessWithHeader(message);
        // check key
        assertEquals(message.key(), wrappedRecord.key());
        // check header
        assertEquals(1, wrappedRecord.headers().toArray().length);
        assertEquals(
                new String(headerValue.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8),
                new String(wrappedRecord.headers().lastHeader(headerKey).value(), StandardCharsets.UTF_8)
        );
        // Check value
        assertNotNull(wrappedRecord.value());
        assertTrue(wrappedRecord.value().isValid());
        assertEquals(value, wrappedRecord.value().getValue());
        assertNull(wrappedRecord.value().getError());
        // Check timestamp
        assertEquals(message.timestamp(), wrappedRecord.timestamp());
    }
    
    @Test
    void shouldCreateWrappedProcessingResultWithHeaderV2() {
        String value = "Value";
        String headerKey = "MSG_HEADER";
        String headerValue = "Header value";
        long timestamp = System.currentTimeMillis();

        Headers headers = new RecordHeaders(Collections.singletonList(
                new RecordHeader(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));
        
        Record<String, ProcessingResult<String, Integer>> wrappedRecord =
                                ProcessingResult.wrapRecordSuccess("key", value, timestamp, headers);
        // check key
        assertEquals("key", wrappedRecord.key());
        // check header
        assertEquals(1, wrappedRecord.headers().toArray().length);
        assertEquals(
                new String(headerValue.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8),
                new String(wrappedRecord.headers().lastHeader(headerKey).value(), StandardCharsets.UTF_8)
        );
        // Check value
        assertNotNull(wrappedRecord.value());
        assertTrue(wrappedRecord.value().isValid());
        assertEquals(value, wrappedRecord.value().getValue());
        assertNull(wrappedRecord.value().getError());
        // Check timestamp
        assertEquals(timestamp, wrappedRecord.timestamp());
    }
    
    @Test
    void shouldCreateFailedProcessingResult() {
        String failedRecordValue = "Failed Value";
        Exception exception = new Exception("Test Exception");

        ProcessingResult<String, String> result = ProcessingResult.<String, String>fail(exception, failedRecordValue);

        assertFalse(result.isValid());
        assertNull(result.getValue());
        assertNotNull(result.getError());
        assertEquals(exception, result.getError().getException());
        assertEquals(failedRecordValue, result.getError().getKafkaRecord());
        assertEquals("No context message", result.getError().getContextMessage());
    }

    @Test
    void shouldCreateWrappedFailedProcessingResult() {
        String key = "key";
        String failedValue = "value";
        long timestamp = System.currentTimeMillis();
        Exception exception = new Exception("Test Exception");

        Record<String, String> message = new Record<>(key, failedValue, timestamp);

        Record<String, ProcessingResult<String, String>> wrappedRecord =
            ProcessingResult.<String, String, String>wrapRecordFailure(exception, message);

        assertEquals(message.key(), wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertFalse(wrappedRecord.value().isValid());
        assertNull(wrappedRecord.value().getValue());
        assertNotNull(wrappedRecord.value().getError());
        assertEquals(exception, wrappedRecord.value().getError().getException());
        assertEquals(failedValue, wrappedRecord.value().getError().getKafkaRecord());
        assertEquals("No context message", wrappedRecord.value().getError().getContextMessage());
        assertEquals(message.timestamp(), wrappedRecord.timestamp());
    }

    @Test
    void shouldProcessingResultBeValid() {
        ProcessingResult<String, Integer> validResult = ProcessingResult.success("Value");
        ProcessingResult<String, Integer> invalidResult1 = ProcessingResult.fail(new Exception(), 42);

        assertTrue(validResult.isValid());
        assertFalse(invalidResult1.isValid());
    }
}

