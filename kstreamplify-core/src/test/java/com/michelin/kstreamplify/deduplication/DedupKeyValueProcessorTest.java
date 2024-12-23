package com.michelin.kstreamplify.deduplication;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingResult;
import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DedupKeyValueProcessorTest {

    private DedupKeyValueProcessor<KafkaError> processor;

    @Mock
    private ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> context;

    @Mock
    private WindowStore<String, KafkaError> windowStore;

    @Mock
    private WindowStoreIterator<KafkaError> windowStoreIterator;

    @BeforeEach
    void setUp() {
        // Create an instance of DedupKeyValueProcessor for testing
        processor = new DedupKeyValueProcessor<>("testStore", Duration.ofHours(1), null);

        // Stub the context.getStateStore method to return the mock store
        when(context.getStateStore("testStore")).thenReturn(windowStore);

        processor.init(context);
    }


    @Test
    void shouldProcessNewRecord() {
        final KafkaError kafkaError = new KafkaError();
        final Record<String, KafkaError> record = new Record<>("key", kafkaError, 0);

        processor.process(record);

        verify(windowStore).put(record.key(), record.value(), record.timestamp());
        verify(context).forward(argThat(arg -> arg.value().getValue().equals(record.value())));

    }

    @Test
    void shouldProcessDuplicate() {
        final KafkaError kafkaError = new KafkaError();
        final Record<String, KafkaError> record = new Record<>("key", kafkaError, 0L);

        // Simulate hasNext() returning true once and then false
        when(windowStoreIterator.hasNext()).thenReturn(true);

        // Simulate the condition to trigger the return statement
        when(windowStoreIterator.next()).thenReturn(KeyValue.pair(0L, kafkaError));

        // Simulate the backwardFetch() method returning the mocked ResultIterator
        when(windowStore.backwardFetch(any(), any(), any())).thenReturn(windowStoreIterator);

        // Call the process method
        processor.process(record);

        verify(windowStore, never()).put(anyString(), any(), anyLong());
        verify(context, never()).forward(any());
    }

    @Test
    void shouldThrowException() {
        final Record<String, KafkaError> message = new Record<>("key", new KafkaError(), 0L);

        when(windowStore.backwardFetch(any(), any(), any())).thenReturn(null)
            .thenThrow(new RuntimeException("Exception..."));
        doThrow(new RuntimeException("Exception...")).when(windowStore).put(anyString(), any(), anyLong());

        // Call the process method
        processor.process(message);

        verify(context).forward(argThat(arg -> arg.value().getError().getContextMessage()
            .equals("Could not figure out what to do with the current payload: "
                + "An unlikely error occurred during deduplication transform")));
    }
}
