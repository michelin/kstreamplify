/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.controller;

import com.michelin.kstreamplify.exception.UnknownKeyException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Controller exception handler. */
@Slf4j
@RestControllerAdvice
public class ControllerExceptionHandler {

    /** Constructor. */
    public ControllerExceptionHandler() {
        // Default constructor
    }

    /**
     * Handle the unknown state store exception.
     *
     * @param e The exception
     * @return The response entity
     */
    @ExceptionHandler(UnknownStateStoreException.class)
    public ResponseEntity<String> handleUnknownStateStoreException(UnknownStateStoreException e) {
        log.error(e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
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
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(e.getMessage());
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
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
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
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
    }
}
