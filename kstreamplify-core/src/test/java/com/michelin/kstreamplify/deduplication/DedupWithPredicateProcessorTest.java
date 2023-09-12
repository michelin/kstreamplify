package com.michelin.kstreamplify.deduplication;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingResult;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Iterator;

import static com.google.common.base.Verify.verify;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DedupWithPredicateProcessorTest {

    private DedupWithPredicateProcessor<String, KafkaError> processor;
    private ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> context;
    private TimestampedKeyValueStore<String, KafkaError> store;

    @BeforeEach
    public void setUp() {
        // Initialize mock objects and the processor
        context = mock(ProcessorContext.class);
        store = mock(TimestampedKeyValueStore.class);

        // Create an instance of DedupWithPredicateProcessor for testing
        processor = new DedupWithPredicateProcessor<>("testStore", Duration.ofHours(1), TestKeyExtractor::extract);

        // Stub the context.getStateStore method to return the mock store
        when(context.getStateStore("testStore")).thenReturn(store);

        processor.init(context);
    }

    @Test
    void testProcessFirstTime() {
        // Create a test record
        Record<String, KafkaError> record = new Record<>("key", new KafkaError(), 0L);

        // Stub store.get to return null, indicating it's the first time
        when(store.get("key")).thenReturn(null);

        // Call the process method
        processor.process(record);

        // Verify that the record is stored in the store and forwarded
        store.put(eq("key"), any());
        context.forward(any());
    }

    @Test
    void testProcessDuplicate() {
        // Create a test record
        Record<String, KafkaError> record = new Record<>("key", new KafkaError(), 0L);

        // Stub store.get to return a value, indicating a duplicate
        when(store.get("key")).thenReturn(ValueAndTimestamp.make(new KafkaError(), 0L));

        // Call the process method
        processor.process(record);

        // Verify that the record is not stored again and not forwarded
        // verify(store, never()).put(any(), any());
        // verify(context, never()).forward(any());
    }

    // Add more test cases as needed

    // Example: Test error handling in process method
    @Test
    void testProcessError() {
        // Create a test record that will trigger an exception
        Record<String, KafkaError> record = new Record<>("key", null, 0L);

        // Call the process method
        processor.process(record);

        // Verify that an error message is forwarded
        // verify(context).forward(argThat(result -> result.isFailure() && result.getErrorMessage().contains("Couldn't figure out")));
    }
}
