package com.michelin.kstreamplify.test;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.error.DlqDeserializationExceptionHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class DlqDeserializationExceptionHandlerTest {

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

    private ConsumerRecord initConsumerRecord() {
        ConsumerRecord record = mock(ConsumerRecord.class);
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

    private DlqDeserializationExceptionHandler initHandler() {
        Future<RecordMetadata> recordMetadataFuture = initRecordMd();

        KafkaProducer kafkaProducer = mock(KafkaProducer.class);
        when(kafkaProducer.send(any())).thenReturn(recordMetadataFuture);

        DlqDeserializationExceptionHandler handler = mock(DlqDeserializationExceptionHandler.class);
        when(handler.getProducer()).thenReturn(kafkaProducer);
        when(handler.handle(any(),any(),any())).thenCallRealMethod();
        handler.configure(new HashMap<>());
        return handler;
    }

    @Test
    public void handleShouldReturnContinue() {
        initCtx("DLQ_TOPIC");
        ProcessorContext processorContext = initProcessorContext();
        ConsumerRecord consumerRecord = initConsumerRecord();
        DlqDeserializationExceptionHandler handler = initHandler();
        when(handler.enrichWithException(any(),any(),any(),any())).thenCallRealMethod();

        DeserializationExceptionHandler.DeserializationHandlerResponse response = handler.handle(processorContext,consumerRecord,new IOException());
        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE, response);
        ctx.close();
    }

    @Test
    public void handleShouldReturnFailBecauseOfNullDlqTopic() {
        initCtx("");
        ProcessorContext processorContext = initProcessorContext();
        ConsumerRecord consumerRecord = initConsumerRecord();
        DlqDeserializationExceptionHandler handler = new DlqDeserializationExceptionHandler();

        DeserializationExceptionHandler.DeserializationHandlerResponse response = handler.handle(processorContext,consumerRecord,null);
        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);
        ctx.close();
    }

    @Test
    public void handleShouldReturnFailBecauseOfException() {
        initCtx("DLQ_TOPIC");
        ProcessorContext processorContext = initProcessorContext();
        ConsumerRecord consumerRecord = initConsumerRecord();
        DlqDeserializationExceptionHandler handler = initHandler();

        DeserializationExceptionHandler.DeserializationHandlerResponse response = handler.handle(processorContext,consumerRecord,new IOException());
        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);
        ctx.close();
    }
}