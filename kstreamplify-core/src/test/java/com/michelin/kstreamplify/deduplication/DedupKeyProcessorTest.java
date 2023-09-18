package com.michelin.kstreamplify.deduplication;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingResult;
import java.time.Duration;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DedupKeyProcessorTest {

    @Mock
    private ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> context;

    @Mock
    private TimestampedKeyValueStore<String, String> dedupTimestampedStore;

    @Test
    void shouldProcessNewRecord() {
        String key = "some-key";

        when(context.getStateStore("dedupStoreName")).thenReturn(dedupTimestampedStore);
        when(dedupTimestampedStore.get(key)).thenReturn(null);

        DedupKeyProcessor<KafkaError> dedupKeyProcessor = new DedupKeyProcessor<>("dedupStoreName", Duration.ZERO);
        dedupKeyProcessor.init(context);

        KafkaError value = new KafkaError();
        Record<String, KafkaError> record = new Record<>(key, value, 0);
        dedupKeyProcessor.process(record);

        verify(dedupTimestampedStore).put(eq(key), any());
    }
}
