package com.michelin.kstreamplify.test;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.converter.AvroToJsonConverter;
import com.michelin.kstreamplify.error.ProcessingError;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class ProcessingErrorTest {

    @Test
    void testProcessingErrorWithStringKafkaRecord() {
        // Arrange
        String contextMessage = "Some context message";
        Exception exception = new Exception("Test Exception");
        String kafkaRecord = "Sample Kafka Record";

        // Act
        ProcessingError processingError = new ProcessingError(exception, contextMessage, kafkaRecord);

        // Assert
        assertEquals(exception, processingError.getException());
        assertEquals(contextMessage, processingError.getContextMessage());
        assertEquals(kafkaRecord, processingError.getKafkaRecord());
    }

//    @Test
    void testProcessingErrorWithGenericRecordKafkaRecord() {
        // Arrange
        Exception exception = new Exception("Test Exception");

        // Mocking the GenericRecord
        GenericRecord genericRecord = Mockito.mock(GenericRecord.class);
        when(genericRecord.toString()).thenReturn("Sample GenericRecord");

        // Mocking the AvroToJsonConverter
        MockedStatic<AvroToJsonConverter> avroToJsonConverter = mockStatic(AvroToJsonConverter.class);
        avroToJsonConverter.when(() -> AvroToJsonConverter.convertRecord(genericRecord)).thenReturn("Sample AvroToJsonConverted");

        // Act
        ProcessingError processingError = new ProcessingError(exception, genericRecord);

        // Assert
        assertEquals(exception, processingError.getException());
        assertEquals("No context message", processingError.getContextMessage());
        assertEquals("Sample AvroToJsonConverted", processingError.getKafkaRecord());
        avroToJsonConverter.close();
    }
}

