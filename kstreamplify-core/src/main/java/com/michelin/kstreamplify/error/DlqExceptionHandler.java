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
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** The class to manage DLQ exception. */
@Slf4j
public abstract class DlqExceptionHandler {
    /** The DLQ producer. */
    @Getter
    protected static Producer<byte[], KafkaError> producer;

    /**
     * Create a producer.
     *
     * @param clientId The producer client id
     * @param configs The producer configs
     */
    public static void instantiateProducer(String clientId, Map<String, ?> configs) {
        Properties properties = new Properties();
        properties.putAll(configs);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        producer = new KafkaProducer<>(properties);
    }

    /**
     * Enrich with exception.
     *
     * @param builder the error builder
     * @param exception the exception to add
     * @param key the record key
     * @param value the record value
     * @return the error enriched by the exception
     */
    public KafkaError.Builder enrichWithException(
            KafkaError.Builder builder, Exception exception, byte[] key, byte[] value) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);

        boolean tooLarge = exception instanceof RecordTooLargeException;

        return builder.setCause(
                        exception.getCause() != null ? exception.getCause().getMessage() : "Unknown cause")
                .setValue(
                        tooLarge
                                ? "The record is too large to be set as value (" + value.length
                                        + " bytes). The key will be used instead"
                                : null)
                .setStack(sw.toString())
                .setByteValue(tooLarge ? ByteBuffer.wrap(key) : ByteBuffer.wrap(value));
    }
}
