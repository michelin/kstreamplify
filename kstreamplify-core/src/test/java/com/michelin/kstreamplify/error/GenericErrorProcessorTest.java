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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import java.util.Optional;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenericErrorProcessorTest {
    private final GenericErrorProcessor<String> errorProcessor = new GenericErrorProcessor<>();

    @Mock
    private FixedKeyProcessorContext<String, KafkaError> mockContext;

    @Mock
    private FixedKeyRecord<String, ProcessingError<String>> mockRecord;

    @Mock
    private RecordMetadata mockRecordMetadata;

    @Test
    void shouldProcessError() {
        when(mockRecord.value())
            .thenReturn(new ProcessingError<>(new RuntimeException("Exception..."), "Context message", "Record"));

        // Given a mock RecordMetadata
        when(mockRecordMetadata.offset()).thenReturn(10L);
        when(mockRecordMetadata.partition()).thenReturn(0);
        when(mockRecordMetadata.topic()).thenReturn("test-topic");

        // Given that the context has a recordMetadata
        when(mockContext.recordMetadata()).thenReturn(Optional.of(mockRecordMetadata));

        // When processing the record
        errorProcessor.init(mockContext);
        errorProcessor.process(mockRecord);

        verify(mockContext).forward(any());
    }
}
