package com.michelin.kstreamplify.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingError;
import org.junit.jupiter.api.Test;

class ProcessingErrorTest {

    @Test
    void shouldCreateProcessingErrorFromStringRecord() {
        String contextMessage = "Some context message";
        Exception exception = new Exception("Test Exception");
        String kafkaRecord = "Sample Kafka Record";

        ProcessingError<String> processingError = new ProcessingError<>(exception, contextMessage, kafkaRecord);

        // Assert
        assertEquals(exception, processingError.getException());
        assertEquals(contextMessage, processingError.getContextMessage());
        assertEquals(kafkaRecord, processingError.getKafkaRecord());
    }

    @Test
    void shouldCreateProcessingErrorWithNoContextMessage() {
        Exception exception = new Exception("Test Exception");
        String kafkaRecord = "Sample Kafka Record";

        ProcessingError<String> processingError = new ProcessingError<>(exception, kafkaRecord);

        // Assert
        assertEquals(exception, processingError.getException());
        assertEquals("No context message", processingError.getContextMessage());
        assertEquals(kafkaRecord, processingError.getKafkaRecord());
    }

    @Test
    void shouldCreateProcessingErrorFromAvroRecord() {
        String contextMessage = "Some context message";
        Exception exception = new Exception("Test Exception");
        KafkaError kafkaRecord = KafkaError.newBuilder()
            .setCause("Cause")
            .setOffset(1L)
            .setPartition(1)
            .setTopic("Topic")
            .setValue("Value")
            .build();

        ProcessingError<KafkaError> processingError = new ProcessingError<>(exception, contextMessage, kafkaRecord);

        assertEquals(exception, processingError.getException());
        assertEquals(contextMessage, processingError.getContextMessage());
        assertEquals("""
            {
              "partition": 1,
              "offset": 1,
              "cause": "Cause",
              "topic": "Topic",
              "value": "Value"
            }""", processingError.getKafkaRecord());
    }
}

