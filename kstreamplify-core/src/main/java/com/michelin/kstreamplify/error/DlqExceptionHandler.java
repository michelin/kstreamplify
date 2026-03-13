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

import com.michelin.kstreamplify.avro.KafkaError;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.RecordTooLargeException;

/** The class to manage DLQ exception. */
@Slf4j
public abstract class DlqExceptionHandler {

    protected String deadLetterQueueTopic;

    /** Constructor. */
    protected DlqExceptionHandler() {}

    /**
     * Enrich a KafkaError with exception details
     *
     * @param builder the error builder
     * @param exception the exception to add
     * @param key record key bytes
     * @param value record value bytes
     * @return enriched builder
     */
    protected KafkaError.Builder enrichWithException(
            KafkaError.Builder builder, Exception exception, byte[] key, byte[] value) {

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);

        boolean tooLarge = exception instanceof RecordTooLargeException;

        return builder.setCause(
                        exception.getCause() != null ? exception.getCause().getMessage() : "Unknown cause")
                .setStack(sw.toString())
                .setValue(
                        tooLarge
                                ? "The record is too large to be set as value (" + value.length
                                        + " bytes). The key will be used instead"
                                : null)
                .setByteValue(tooLarge ? ByteBuffer.wrap(key) : ByteBuffer.wrap(value));
    }

    /** Check if DLQ topic is defined */
    protected boolean isDlqDefined() {
        return StringUtils.isNotBlank(deadLetterQueueTopic);
    }
}
