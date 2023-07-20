package com.michelin.kstreamplify.deduplication;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingResult;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;

import static org.mockito.Mockito.*;

class DedupKeyProcessorTest {

    @Mock
    private ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> context;

    @Mock
    private TimestampedKeyValueStore<String, String> dedupTimestampedStore;

    @InjectMocks
    private DedupKeyProcessor<KafkaError> dedupKeyProcessor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(context.getStateStore("dedupStoreName")).thenReturn(dedupTimestampedStore);
    }

    @Test
    void testProcessNewRecord() {
        String key = "some-key";
        KafkaError value = new KafkaError();

        Record<String, KafkaError> record = new Record<>(key, value, 0);

        when(dedupTimestampedStore.get(key)).thenReturn(null);

        DedupKeyProcessor<KafkaError> dedupKeyProcessor = new DedupKeyProcessor<>("dedupStoreName",Duration.ZERO);
        dedupKeyProcessor.init(context);
        dedupKeyProcessor.process(record);

        verify(dedupTimestampedStore).put(eq(key), any());
    }

}
