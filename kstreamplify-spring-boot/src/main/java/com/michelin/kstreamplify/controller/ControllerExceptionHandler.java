package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.http.exception.InstanceNotReadyException;
import com.michelin.kstreamplify.http.exception.StoreNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
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
     * Handle the store not found exception.
     *
     * @param e The exception
     * @return The response entity
     */
    @ExceptionHandler(StoreNotFoundException.class)
    public ResponseEntity<String> handleStoreNotFoundException(StoreNotFoundException e) {
        log.error(e.getMessage(), e);
        return ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(e.getMessage());
    }

    /**
     * Handle the instance not ready exception.
     *
     * @param e The exception
     * @return The response entity
     */
    @ExceptionHandler(InstanceNotReadyException.class)
    public ResponseEntity<String> handleInstanceNotReadyException(InstanceNotReadyException e) {
        log.error(e.getMessage(), e);
        return ResponseEntity
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(e.getMessage());
    }
}
