package com.michelin.kstreamplify.rest;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import org.junit.jupiter.api.Test;

class DefaultProbeControllerTest {
    @Test
    void shouldCreateServerWithDefaultHostAndPort() {
        DefaultProbeController controller = new DefaultProbeController(new KafkaStreamsInitializer());

        assertNotNull(controller.server.getAddress().getAddress().getHostName());
        assertNotEquals(0, controller.server.getAddress().getPort());
    }
}
