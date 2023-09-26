package com.michelin.kstreamplify.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.error.DlqExceptionHandler;
import com.michelin.kstreamplify.error.DlqProductionExceptionHandler;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DlqProductionExceptionHandlerTest {
    @Mock
    private ProducerRecord<byte[], byte[]> record;

    private Producer<byte[], KafkaError> producer;

    private DlqProductionExceptionHandler handler;

    @BeforeEach
    void setUp() {
        Serializer<KafkaError> serializer = (Serializer) new KafkaAvroSerializer();
        serializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        producer = new MockProducer<>(true, new ByteArraySerializer(), serializer);

        KafkaStreamsExecutionContext.setDlqTopicName(null);
    }

    @Test
    void shouldReturnFailIfNoDlq() {
        handler = new DlqProductionExceptionHandler(producer);

        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
            handler.handle(record, new RuntimeException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL, response);
    }

    @Test
    void shouldReturnContinueOnExceptionDuringHandle() {
        handler = new DlqProductionExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");
        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
            handler.handle(record, new KafkaException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldReturnContinueOnKafkaException() {
        handler = new DlqProductionExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        when(record.key()).thenReturn("key".getBytes(StandardCharsets.UTF_8));
        when(record.value()).thenReturn("value".getBytes(StandardCharsets.UTF_8));
        when(record.topic()).thenReturn("topic");

        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
            handler.handle(record, new KafkaException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE, response);
    }

    @Test
    void shouldReturnFailOnRetriableException() {
        handler = new DlqProductionExceptionHandler(producer);
        KafkaStreamsExecutionContext.setDlqTopicName("DlqTopic");

        ProductionExceptionHandler.ProductionExceptionHandlerResponse response =
            handler.handle(record, new RetriableCommitFailedException("Exception..."));

        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.FAIL, response);
    }

    @Test
    void shouldConfigure() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("schema.registry.url", "localhost:8080");
        configs.put("acks", "all");

        handler = new DlqProductionExceptionHandler();
        handler.configure(configs);

        assertTrue(DlqExceptionHandler.getProducer() instanceof KafkaProducer<byte[], KafkaError>);
    }
}

