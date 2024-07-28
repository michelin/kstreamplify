package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.exception.UnknownKeyException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Controller exception handler.
 */
@Slf4j
@RestControllerAdvice
public class ControllerExceptionHandler {

    /**
     * Handle the unknown state store exception.
     *
     * @param e The exception
     * @return The response entity
     */
    @ExceptionHandler(UnknownStateStoreException.class)
    public ResponseEntity<String> handleUnknownStateStoreException(UnknownStateStoreException e) {
        log.error(e.getMessage(), e);
        return ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(e.getMessage());
    }

    /**
     * Handle the stream not started exception.
     *
     * @param e The exception
     * @return The response entity
     */
    @ExceptionHandler(StreamsNotStartedException.class)
    public ResponseEntity<String> handleStreamsNotStartedException(StreamsNotStartedException e) {
        log.error(e.getMessage(), e);
        return ResponseEntity
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(e.getMessage());
    }

    /**
     * Handle the unknown key exception.
     *
     * @param e The exception
     * @return The response entity
     */
    @ExceptionHandler(UnknownKeyException.class)
    public ResponseEntity<String> handleUnknownKeyException(UnknownKeyException e) {
        log.error(e.getMessage(), e);
        return ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(e.getMessage());
    }

    /**
     * Handle the illegal argument exception.
     *
     * @param e The exception
     * @return The response entity
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleIllegalArgumentException(IllegalArgumentException e) {
        log.error(e.getMessage(), e);
        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body(e.getMessage());
    }
}
