package com.michelin.kstreamplify.deduplication;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingResult;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DedupKeyProcessorTest {

    private DedupKeyProcessor<KafkaError> processor;

    @Mock
    private ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> context;

    @Mock
    private WindowStore<String, String> windowStore;

    @Mock
    private WindowStoreIterator<String> windowStoreIterator;

    @BeforeEach
    void setUp() {
        // Create an instance of DedupKeyProcessor for testing
        processor = new DedupKeyProcessor<>("testStore", Duration.ofHours(1), null);

        // Stub the context.getStateStore method to return the mock store
        when(context.getStateStore("testStore")).thenReturn(windowStore);

        processor.init(context);
    }

    @Test
    void shouldNotCopyStoreWhenInit() {
        processor.init(context);
        verify(windowStore, never()).put(anyString(), anyString(), anyLong());

    }

    @Test
    void shouldCopyStoreWhenInit() {
        DedupKeyProcessor<KafkaError> dedupKeyProcessor = new DedupKeyProcessor<>("testStore",
                Duration.ofHours(1), "timestampStore");

        ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> processorContext = Mockito.mock(ProcessorContext.class);

        WindowStore<String, String> windowStore = Mockito.mock(WindowStore.class);
        TimestampedKeyValueStore<String, String> timestampedKeyValueStore = Mockito.mock(TimestampedKeyValueStore.class);

        when(processorContext.getStateStore("testStore")).thenReturn(windowStore);

        when(processorContext.getStateStore("timestampStore")).thenReturn(timestampedKeyValueStore);

        long currentTimeStamp = System.currentTimeMillis();

        KeyValueIterator<String, ValueAndTimestamp<String>> iterator = getStringValueAndTimestampKeyValueIterator(currentTimeStamp);

        when(timestampedKeyValueStore.all()).thenReturn(iterator);

        dedupKeyProcessor.init(processorContext);

        verify(windowStore).put("key1", "value1", currentTimeStamp - (2 * 60 * 60 * 1000));
        verify(windowStore).put("key2", "value2", currentTimeStamp);
        verify(windowStore).put("key3", "value3", currentTimeStamp);

        // re call the init to verify the delete part
        dedupKeyProcessor.init(processorContext);
        verify(windowStore).put("key1", "value1", currentTimeStamp - (2 * 60 * 60 * 1000));

    }

    private static @NotNull KeyValueIterator<String, ValueAndTimestamp<String>> getStringValueAndTimestampKeyValueIterator(long currentTimeStamp) {
        List<KeyValue<String, ValueAndTimestamp<String>>> data = new ArrayList<>();

        data.add(new KeyValue<>("key1", ValueAndTimestamp.make("value1", currentTimeStamp - (2 * 60 * 60 * 1000))));
        data.add(new KeyValue<>("key2", ValueAndTimestamp.make("value2", currentTimeStamp)));
        data.add(new KeyValue<>("key3", ValueAndTimestamp.make("value3", currentTimeStamp)));

        return new KeyValueIterator<>() {
            private final Iterator<KeyValue<String, ValueAndTimestamp<String>>> underlyingIterator = data.iterator();

            @Override
            public boolean hasNext() {
                return underlyingIterator.hasNext();
            }

            @Override
            public KeyValue<String, ValueAndTimestamp<String>> next() {
                return underlyingIterator.next();
            }

            @Override
            public void close() {
                // nothing to do
            }

            @Override
            public String peekNextKey() {
                return "";
            }
        };
    }

    @Test
    void shouldProcessNewRecord() {
        final KafkaError kafkaError = new KafkaError();
        final Record<String, KafkaError> record = new Record<>("key", kafkaError, 0);

        processor.process(record);

        verify(windowStore).put("key", "key", record.timestamp());
        verify(context).forward(argThat(arg -> arg.value().getValue().equals(record.value())));

    }

    @Test
    void shouldProcessDuplicate() {
        final KafkaError kafkaError = new KafkaError();
        final Record<String, KafkaError> record = new Record<>("key", kafkaError, 0L);

        // Simulate hasNext() returning true once and then false
        when(windowStoreIterator.hasNext()).thenReturn(true);

        // Simulate the condition to trigger the return statement
        when(windowStoreIterator.next()).thenReturn(KeyValue.pair(0L, "key"));

        // Simulate the backwardFetch() method returning the mocked ResultIterator
        when(windowStore.backwardFetch(any(), any(), any())).thenReturn(windowStoreIterator);

        // Call the process method
        processor.process(record);

        verify(windowStore, never()).put(anyString(), any(), anyLong());
        verify(context, never()).forward(any());
    }

    @Test
    void shouldThrowException() {
        Record<String, KafkaError> record = new Record<>("key", new KafkaError(), 0L);

        when(windowStore.backwardFetch(any(), any(), any())).thenReturn(null)
                .thenThrow(new RuntimeException("Exception..."));
        doThrow(new RuntimeException("Exception...")).when(windowStore).put(anyString(), any(), anyLong());

        processor.process(record);

        verify(context).forward(argThat(arg -> arg.value().getError().getContextMessage()
                .equals("Could not figure out what to do with the current payload: "
                        + "An unlikely error occurred during deduplication transform")));
    }

}
