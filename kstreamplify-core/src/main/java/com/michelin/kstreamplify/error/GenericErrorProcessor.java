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
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * Generic error processor.
 *
 * @param <V> The type of the failed record
 */
class GenericErrorProcessor<V> extends ContextualFixedKeyProcessor<String, ProcessingError<V>, KafkaError> {
    /**
     * Process the error.
     *
     * @param fixedKeyRecord the record to process an error
     */
    @Override
    public void process(FixedKeyRecord<String, ProcessingError<V>> fixedKeyRecord) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        fixedKeyRecord.value().getException().printStackTrace(pw);

        RecordMetadata recordMetadata = context().recordMetadata().orElse(null);

        KafkaError error = KafkaError.newBuilder()
                .setCause(fixedKeyRecord.value().getException().getMessage())
                .setContextMessage(fixedKeyRecord.value().getContextMessage())
                .setOffset(recordMetadata != null ? recordMetadata.offset() : -1)
                .setPartition(recordMetadata != null ? recordMetadata.partition() : -1)
                .setStack(sw.toString())
                .setTopic(
                        recordMetadata != null && recordMetadata.topic() != null
                                ? recordMetadata.topic()
                                : "Outside topic context")
                .setValue(fixedKeyRecord.value().getKafkaRecord())
                .setApplicationId(context().applicationId())
                .build();

        context().forward(fixedKeyRecord.withValue(error));
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        // may close resource opened in init
    }
}
