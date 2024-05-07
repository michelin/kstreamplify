package com.michelin.kstreamplify.http.server;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import org.junit.jupiter.api.Test;

class KafkaStreamsHttpServerTest {
    @Test
    void shouldCreateServerWithDefaultHostAndPort() {
        KafkaStreamsHttpServer server = new KafkaStreamsHttpServer(new KafkaStreamsInitializer());
        server.start();

        assertNotNull(server.server.getAddress().getHostName());
        assertNotEquals(0, server.server.getAddress().getPort());
    }
}
