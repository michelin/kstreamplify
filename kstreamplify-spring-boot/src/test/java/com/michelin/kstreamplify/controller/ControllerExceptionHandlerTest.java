package com.michelin.kstreamplify.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

class ControllerExceptionHandlerTest {
    private final ControllerExceptionHandler controllerExceptionHandler = new ControllerExceptionHandler();

    @Test
    void shouldHandleUnknownStateStoreException() {
        UnknownStateStoreException e = new UnknownStateStoreException("message");

        ResponseEntity<String> response = controllerExceptionHandler.handleUnknownStateStoreException(e);

        assertEquals("message", response.getBody());
        assertEquals(404, response.getStatusCode().value());
    }

    @Test
    void shouldHandleStreamsNotStartedException() {
        StreamsNotStartedException e = new StreamsNotStartedException("message");

        ResponseEntity<String> response = controllerExceptionHandler.handleStreamsNotStartedException(e);

        assertEquals("message", response.getBody());
        assertEquals(503, response.getStatusCode().value());
    }
}
