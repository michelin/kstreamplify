package com.michelin.kstreamplify.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DlqDeserializationExceptionHandlerTest {
    @Mock
    private ConsumerRecord<byte[], byte[]> record;

    @Mock
    private ProcessorContext processorContext;

    private Producer<byte[], KafkaError> producer;

    private DlqDeserializationExceptionHandler handler;

    @BeforeEach
    void setUp() {
        Serializer<KafkaError> serializer = (Serializer) new KafkaAvroSerializer();
        serializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        producer = new MockProducer<>(true, new ByteArraySerializer(), serializer);

        KafkaStreamsExecutionContext.setDlqTopicName(null);
    }

    @Test
    void shouldReturnFailIfNoDlq() {
        handler = new DlqDeserializationExceptionHandler(producer);

        DeserializationExceptionHandler.DeserializationHandlerResponse response =
            handler.handle(processorContext, record, new RuntimeException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnFailOnExceptionDuringHandle() {
        handler = new DlqDeserializationExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");
        DeserializationExceptionHandler.DeserializationHandlerResponse response =
            handler.handle(processorContext, record, new KafkaException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnContinueOnKafkaException() {
        handler = new DlqDeserializationExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        when(record.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(record.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(record.topic()).thenReturn("topic");

        DeserializationExceptionHandler.DeserializationHandlerResponse response =
            handler.handle(processorContext, record, new KafkaException("Exception..."));

        assertEquals(DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldConfigure() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("schema.registry.url", "localhost:8080");
        configs.put("acks", "all");

        handler = new DlqDeserializationExceptionHandler(null);
        handler.configure(configs);

        assertTrue(DlqExceptionHandler.getProducer() instanceof KafkaProducer<byte[], KafkaError>);
    }

    @Test
    void shouldEnrichWithException() {
        KafkaError.Builder kafkaError = KafkaError.newBuilder()
            .setTopic("topic")
            .setStack("stack")
            .setPartition(0)
            .setOffset(0)
            .setCause("cause")
            .setValue("value");

        handler = new DlqDeserializationExceptionHandler();
        KafkaError.Builder enrichedBuilder = handler.enrichWithException(kafkaError,
            new RuntimeException("Exception..."), "key".getBytes(StandardCharsets.UTF_8),
            "value".getBytes(StandardCharsets.UTF_8));

        KafkaError error = enrichedBuilder.build();
        assertEquals("Unknown cause", error.getCause());
        assertNull(error.getContextMessage());
    }

    @Test
    void shouldEnrichWithRecordTooLargeException() {
        KafkaError.Builder kafkaError = KafkaError.newBuilder()
            .setTopic("topic")
            .setStack("stack")
            .setPartition(0)
            .setOffset(0)
            .setCause("cause")
            .setValue("value");

        handler = new DlqDeserializationExceptionHandler();
        KafkaError.Builder enrichedBuilder = handler.enrichWithException(kafkaError,
            new RecordTooLargeException("Exception..."), "key".getBytes(StandardCharsets.UTF_8),
            "value".getBytes(StandardCharsets.UTF_8));

        KafkaError error = enrichedBuilder.build();
        assertEquals("Unknown cause", error.getCause());
        assertEquals("The record is too large to be set as value (5 bytes). "
            + "The key will be used instead", error.getValue());
    }
}
