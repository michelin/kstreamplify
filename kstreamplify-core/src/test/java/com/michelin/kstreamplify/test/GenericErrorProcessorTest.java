package com.michelin.kstreamplify.test;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.GenericErrorProcessor;
import com.michelin.kstreamplify.error.ProcessingError;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class GenericErrorProcessorTest {

    private GenericErrorProcessor<String> errorProcessor;
    private FixedKeyProcessorContext<String, KafkaError> mockContext;
    private FixedKeyRecord<String, ProcessingError<String>> mockRecord;
    private RecordMetadata mockRecordMetadata;

    @BeforeEach
    public void setUp() {
        errorProcessor = new GenericErrorProcessor<>();
        mockContext = mock(FixedKeyProcessorContext.class);
        mockRecord = mock(FixedKeyRecord.class);
        mockRecordMetadata = mock(RecordMetadata.class);
    }

    @Test
    void testProcess() {
        // Given a mock ProcessingError
        ProcessingError<String> mockProcessingError = mock(ProcessingError.class);
        when(mockProcessingError.getException()).thenReturn(new RuntimeException("Test Exception"));
        when(mockProcessingError.getContextMessage()).thenReturn("Test Context Message");
        when(mockProcessingError.getKafkaRecord()).thenReturn("Test Kafka Record");

        // Given a mock FixedKeyRecord
        when(mockRecord.value()).thenReturn(mockProcessingError);

        // Given a mock RecordMetadata
        when(mockRecordMetadata.offset()).thenReturn(10L);
        when(mockRecordMetadata.partition()).thenReturn(0);
        when(mockRecordMetadata.topic()).thenReturn("test-topic");

        // Given that the context has a recordMetadata
        when(mockContext.recordMetadata()).thenReturn(Optional.of(mockRecordMetadata));

        // When processing the record
        errorProcessor.init(mockContext);
        errorProcessor.process(mockRecord);

        assertNotNull(errorProcessor);
    }
}
