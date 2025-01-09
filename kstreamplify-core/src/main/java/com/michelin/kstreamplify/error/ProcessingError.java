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

package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.converter.AvroToJsonConverter;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;

/**
 * The processing error class.
 *
 * @param <V> The type of the failed record
 */
@Getter
public class ProcessingError<V> {
    /**
     * The exception that occurred.
     */
    private final Exception exception;

    /**
     * The failed Kafka record.
     */
    private final String kafkaRecord;

    /**
     * A context message defined when the error is caught.
     */
    private final String contextMessage;

    /**
     * Constructor.
     *
     * @param exception      The exception
     * @param contextMessage The context message
     * @param kafkaRecord    The failed Kafka record
     */
    public ProcessingError(Exception exception, String contextMessage, V kafkaRecord) {
        this.exception = exception;
        this.contextMessage = contextMessage;

        if (kafkaRecord instanceof GenericRecord genericRecord) {
            this.kafkaRecord = AvroToJsonConverter.convertRecord(genericRecord);
        } else {
            this.kafkaRecord = String.valueOf(kafkaRecord);
        }
    }

    /**
     * Constructor.
     *
     * @param exception   The exception
     * @param kafkaRecord The failed Kafka record
     */
    public ProcessingError(Exception exception, V kafkaRecord) {
        this(exception, "No context message", kafkaRecord);
    }
}
