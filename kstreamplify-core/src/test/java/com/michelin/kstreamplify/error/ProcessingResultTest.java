package com.michelin.kstreamplify.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        Record<String, String> record = new Record<>("key", value, timestamp);
        Record<String, ProcessingResult<String, Integer>> wrappedRecord = ProcessingResult.wrapRecordSuccess(record);

        assertEquals(record.key(), wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertTrue(wrappedRecord.value().isValid());
        assertEquals(value, wrappedRecord.value().getValue());
        assertNull(wrappedRecord.value().getError());
        assertEquals(record.timestamp(), wrappedRecord.timestamp());
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

        Record<String, String> record = new Record<>(key, failedValue, timestamp);

        Record<String, ProcessingResult<String, String>> wrappedRecord =
            ProcessingResult.<String, String, String>wrapRecordFailure(exception, record);

        assertEquals(record.key(), wrappedRecord.key());
        assertNotNull(wrappedRecord.value());
        assertFalse(wrappedRecord.value().isValid());
        assertNull(wrappedRecord.value().getValue());
        assertNotNull(wrappedRecord.value().getError());
        assertEquals(exception, wrappedRecord.value().getError().getException());
        assertEquals(failedValue, wrappedRecord.value().getError().getKafkaRecord());
        assertEquals("No context message", wrappedRecord.value().getError().getContextMessage());
        assertEquals(record.timestamp(), wrappedRecord.timestamp());
    }

    @Test
    void shouldProcessingResultBeValid() {
        ProcessingResult<String, Integer> validResult = ProcessingResult.success("Value");
        ProcessingResult<String, Integer> invalidResult1 = ProcessingResult.fail(new Exception(), 42);

        assertTrue(validResult.isValid());
        assertFalse(invalidResult1.isValid());
    }
}

