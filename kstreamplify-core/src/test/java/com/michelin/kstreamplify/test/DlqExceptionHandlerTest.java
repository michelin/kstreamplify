package com.michelin.kstreamplify.test;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.DlqDeserializationExceptionHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DlqExceptionHandlerTest {

    @Test
    void testInstantiateProducer() {
        String clientId = "test-client";
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("schema.registry.url", "localhost:8080");
        configs.put("acks", "all");

        DlqDeserializationExceptionHandler handler = new DlqDeserializationExceptionHandler();

        handler.instantiateProducer(clientId, configs);

        KafkaProducer<byte[], KafkaError> producer = handler.getProducer();
        assertTrue(producer != null);
    }
}
