package com.michelin.kstreamplify.deduplication;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingResult;
import java.time.Duration;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DedupWithPredicateProcessorTest {

    private DedupWithPredicateProcessor<String, KafkaError> processor;

    @Mock
    private ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> context;

    @Mock
    private TimestampedKeyValueStore<String, KafkaError> store;

    @BeforeEach
    void setUp() {
        // Create an instance of DedupWithPredicateProcessor for testing
        processor = new DedupWithPredicateProcessor<>("testStore", Duration.ofHours(1), TestKeyExtractor::extract);

        // Stub the context.getStateStore method to return the mock store
        when(context.getStateStore("testStore")).thenReturn(store);

        processor.init(context);
    }

    @Test
    void shouldProcessFirstTime() {
        // Create a test record
        Record<String, KafkaError> record = new Record<>("key", new KafkaError(), 0L);

        // Stub store.get to return null, indicating it's the first time
        when(store.get(any())).thenReturn(null);

        // Call the process method
        processor.process(record);

        verify(store).put(eq(""), argThat(arg -> arg.value().equals(record.value())));
        verify(context).forward(argThat(arg -> arg.value().getValue().equals(record.value())));
    }

    @Test
    void shouldProcessDuplicate() {
        // Create a test record
        Record<String, KafkaError> record = new Record<>("key", new KafkaError(), 0L);

        // Stub store.get to return a value, indicating a duplicate
        when(store.get("")).thenReturn(ValueAndTimestamp.make(new KafkaError(), 0L));

        // Call the process method
        processor.process(record);

        verify(store, never()).put(any(), any());
        verify(context, never()).forward(any());
    }

    @Test
    void shouldThrowException() {
        // Create a test record
        Record<String, KafkaError> record = new Record<>("key", new KafkaError(), 0L);

        when(store.get(any())).thenReturn(null);
        doThrow(new RuntimeException("Exception...")).when(store).put(any(), any());

        // Call the process method
        processor.process(record);

        verify(context).forward(argThat(arg -> arg.value().getError().getContextMessage()
            .equals("Couldn't figure out what to do with the current payload: "
                + "An unlikely error occurred during deduplication transform")));
    }

    public static class TestKeyExtractor {
        public static <V extends SpecificRecord> String extract(V v) {
            return "";
        }
    }
}
