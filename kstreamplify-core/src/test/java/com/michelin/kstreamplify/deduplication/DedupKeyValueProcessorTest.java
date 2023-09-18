package com.michelin.kstreamplify.deduplication;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingResult;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DedupKeyValueProcessorTest {

    @Mock
    private ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> context;

    @Mock
    private WindowStore<String, KafkaError> windowStore;

    @InjectMocks
    private DedupKeyValueProcessor<KafkaError> dedupKeyValueProcessor;

    @Test
    void shouldProcessNewRecord() {
        String key = "some-key";
        KafkaError value = new KafkaError();

        Record<String, KafkaError> record = new Record<>(key, value, 0);

        when(context.getStateStore("dedupStoreName")).thenReturn(windowStore);

        DedupKeyValueProcessor<KafkaError> dedupKeyValueProcessor = new DedupKeyValueProcessor<>("dedupStoreName",Duration.ZERO);
        dedupKeyValueProcessor.init(context);
        dedupKeyValueProcessor.process(record);

        verify(windowStore).put(record.key(), record.value(), record.timestamp());
    }
}
