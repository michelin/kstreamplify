package com.michelin.kstreamplify.deduplication;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingResult;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DedupKeyProcessorTest {

    @Mock
    private ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> context;

    @Mock
    private TimestampedKeyValueStore<String, String> dedupTimestampedStore;

    @InjectMocks
    private DedupKeyProcessor<KafkaError> dedupKeyProcessor;

    @Test
    void shouldProcessNewRecord() {
        String key = "some-key";
        KafkaError value = new KafkaError();

        Record<String, KafkaError> record = new Record<>(key, value, 0);

        when(context.getStateStore("dedupStoreName")).thenReturn(dedupTimestampedStore);
        when(dedupTimestampedStore.get(key)).thenReturn(null);

        DedupKeyProcessor<KafkaError> dedupKeyProcessor = new DedupKeyProcessor<>("dedupStoreName",Duration.ZERO);
        dedupKeyProcessor.init(context);
        dedupKeyProcessor.process(record);

        verify(dedupTimestampedStore).put(eq(key), any());
    }
}
