package com.michelin.kstreamplify.rest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.michelin.kstreamplify.error.DlqExceptionHandler;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DlqExceptionHandlerTest {

    @Test
    void shouldInstantiateProducer() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("schema.registry.url", "localhost:8080");
        configs.put("acks", "all");

        DlqExceptionHandler.instantiateProducer("test-client", configs);

        assertNotNull(DlqExceptionHandler.getProducer());
    }
}
