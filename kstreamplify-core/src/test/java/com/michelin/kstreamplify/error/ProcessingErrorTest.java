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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kstreamplify.avro.KafkaError;
import org.junit.jupiter.api.Test;

class ProcessingErrorTest {

    @Test
    void shouldCreateProcessingErrorFromStringRecord() {
        String contextMessage = "Some context message";
        Exception exception = new Exception("Test Exception");
        String kafkaRecord = "Sample Kafka Record";

        ProcessingError<String> processingError = new ProcessingError<>(exception, contextMessage, kafkaRecord);

        // Assert
        assertEquals(exception, processingError.getException());
        assertEquals(contextMessage, processingError.getContextMessage());
        assertEquals(kafkaRecord, processingError.getKafkaRecord());
    }

    @Test
    void shouldCreateProcessingErrorWithNoContextMessage() {
        Exception exception = new Exception("Test Exception");
        String kafkaRecord = "Sample Kafka Record";

        ProcessingError<String> processingError = new ProcessingError<>(exception, kafkaRecord);

        // Assert
        assertEquals(exception, processingError.getException());
        assertEquals("No context message", processingError.getContextMessage());
        assertEquals(kafkaRecord, processingError.getKafkaRecord());
    }

    @Test
    void shouldCreateProcessingErrorFromAvroRecord() {
        String contextMessage = "Some context message";
        Exception exception = new Exception("Test Exception");
        KafkaError kafkaRecord = KafkaError.newBuilder()
            .setCause("Cause")
            .setOffset(1L)
            .setPartition(1)
            .setTopic("Topic")
            .setValue("Value")
            .setApplicationId("ApplicationId")
            .build();

        ProcessingError<KafkaError> processingError = new ProcessingError<>(exception, contextMessage, kafkaRecord);

        assertEquals(exception, processingError.getException());
        assertEquals(contextMessage, processingError.getContextMessage());
        assertEquals("""
            {
              "partition": 1,
              "offset": 1,
              "cause": "Cause",
              "topic": "Topic",
              "applicationId": "ApplicationId",
              "value": "Value"
            }""", processingError.getKafkaRecord());
    }
}

