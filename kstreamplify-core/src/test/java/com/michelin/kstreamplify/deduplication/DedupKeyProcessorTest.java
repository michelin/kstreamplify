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
package com.michelin.kstreamplify.deduplication;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.error.ProcessingResult;
import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DedupKeyProcessorTest {

    private DedupKeyProcessor<KafkaError> processor;

    @Mock
    private ProcessorContext<String, ProcessingResult<KafkaError, KafkaError>> context;

    @Mock
    private WindowStore<String, String> windowStore;

    @Mock
    private WindowStoreIterator<String> windowStoreIterator;

    @BeforeEach
    void setUp() {
        // Create an instance of DedupWithPredicateProcessor for testing
        processor = new DedupKeyProcessor<>("testStore", Duration.ofHours(1));

        // Stub the context.getStateStore method to return the mock store
        when(context.getStateStore("testStore")).thenReturn(windowStore);

        processor.init(context);
    }

    @Test
    void shouldProcessNewRecord() {
        final KafkaError kafkaError = new KafkaError();
        final Record<String, KafkaError> record = new Record<>("key", kafkaError, 0);

        processor.process(record);

        verify(windowStore).put("key", "key", record.timestamp());
        verify(context).forward(argThat(arg -> arg.value().getValue().equals(record.value())));
    }

    @Test
    void shouldProcessDuplicate() {
        final KafkaError kafkaError = new KafkaError();
        final Record<String, KafkaError> record = new Record<>("key", kafkaError, 0L);

        // Simulate hasNext() returning true once and then false
        when(windowStoreIterator.hasNext()).thenReturn(true);

        // Simulate the condition to trigger the return statement
        when(windowStoreIterator.next()).thenReturn(KeyValue.pair(0L, "key"));

        // Simulate the backwardFetch() method returning the mocked ResultIterator
        when(windowStore.backwardFetch(any(), any(), any())).thenReturn(windowStoreIterator);

        // Call the process method
        processor.process(record);

        verify(windowStore, never()).put(anyString(), any(), anyLong());
        verify(context, never()).forward(any());
    }

    @Test
    void shouldThrowException() {
        Record<String, KafkaError> record = new Record<>("key", new KafkaError(), 0L);

        when(windowStore.backwardFetch(any(), any(), any()))
                .thenReturn(null)
                .thenThrow(new RuntimeException("Exception..."));
        doThrow(new RuntimeException("Exception...")).when(windowStore).put(anyString(), any(), anyLong());

        processor.process(record);

        verify(context).forward(argThat(arg -> arg.value()
                .getError()
                .getContextMessage()
                .equals("Could not figure out what to do with the current payload: "
                        + "An unlikely error occurred during deduplication transform")));
    }
}
