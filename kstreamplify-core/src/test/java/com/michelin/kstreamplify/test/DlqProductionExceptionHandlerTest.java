package com.michelin.kstreamplify.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.error.DlqDeserializationExceptionHandler;
import com.michelin.kstreamplify.error.DlqExceptionHandler;
import com.michelin.kstreamplify.error.DlqProductionExceptionHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DlqProductionExceptionHandlerTest {

    private MockedStatic<KafkaStreamsExecutionContext> ctx;

    private void initCtx(String dlqTopic) {
        ctx = mockStatic(KafkaStreamsExecutionContext.class);
        ctx.when(KafkaStreamsExecutionContext::getDlqTopicName).thenReturn(dlqTopic);
    }

    private ProcessorContext initProcessorContext() {
        ProcessorContext ctx = mock(ProcessorContext.class);
        when(ctx.offset()).thenReturn((long) 0);
        when(ctx.partition()).thenReturn(1);
        when(ctx.topic()).thenReturn("TOPIC");
        return ctx;
    }

    private ProducerRecord initProducerRecord() {
        ProducerRecord record = mock(ProducerRecord.class);
        when(record.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(record.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(record.topic()).thenReturn("TOPIC");
        return record;
    }

    private Future<RecordMetadata> initRecordMd() {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        return executor.submit(new Callable<RecordMetadata>() {
            @Override
            public RecordMetadata call() throws Exception {
                return new RecordMetadata(new TopicPartition("TOPIC", 0), 0, 0, 0, 0, 0);
            }
        });
    }

    private DlqProductionExceptionHandler initHandler() {
        Future<RecordMetadata> recordMetadataFuture = initRecordMd();

        KafkaProducer kafkaProducer = mock(KafkaProducer.class);
        when(kafkaProducer.send(any())).thenReturn(recordMetadataFuture);

        DlqProductionExceptionHandler handler = mock(DlqProductionExceptionHandler.class);
        when(handler.getProducer()).thenReturn(kafkaProducer);
        when(handler.handle(any(),any())).thenCallRealMethod();
        doCallRealMethod().when(handler).configure(any());
        handler.configure(new HashMap<>());
        return handler;
    }

    @Test
    public void handleShouldReturnContinue() {
        initCtx("DLQ_TOPIC");
        ProcessorContext processorContext = initProcessorContext();
        ProducerRecord producerRecord = initProducerRecord();
        DlqProductionExceptionHandler handler = initHandler();
        when(handler.enrichWithException(any(),any(),any(),any())).thenCallRealMethod();

        ProductionExceptionHandlerResponse response = handler.handle(producerRecord,new IOException());
        assertEquals(ProductionExceptionHandlerResponse.CONTINUE, response);
        ctx.close();
    }

    @Test
    public void handleShouldReturnFailBecauseOfNullDlqTopic() {
        initCtx("");
        ProcessorContext processorContext = initProcessorContext();
        ProducerRecord producerRecord = initProducerRecord();
        DlqProductionExceptionHandler handler = new DlqProductionExceptionHandler();

        ProductionExceptionHandlerResponse response = handler.handle(producerRecord,null);
        assertEquals(ProductionExceptionHandlerResponse.FAIL, response);
        ctx.close();
    }

    @Test
    public void handleShouldReturnContinueBecauseOfException() {
        initCtx("DLQ_TOPIC");
        ProcessorContext processorContext = initProcessorContext();
        ProducerRecord producerRecord = initProducerRecord();
        DlqProductionExceptionHandler handler = initHandler();

        ProductionExceptionHandlerResponse response = handler.handle(producerRecord,new IOException());
        assertEquals(ProductionExceptionHandlerResponse.CONTINUE, response);
        ctx.close();
    }

    @Test
    public void testConfigure() {
        var handler = initHandler();

        Map<String, Object> configs = new HashMap<>();
        when(handler.getProducer()).thenReturn(null);
        try (var mockHandler = mockStatic(DlqExceptionHandler.class)) {
            handler.configure(configs);
        }
    }
}

