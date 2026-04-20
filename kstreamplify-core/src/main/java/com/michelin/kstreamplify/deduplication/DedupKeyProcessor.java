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

import java.time.Duration;
import java.time.Instant;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Processor class for the deduplication mechanism on keys of a given topic.
 *
 * @param <V> The type of the value
 */
public class DedupKeyProcessor<V extends SpecificRecord> implements Processor<String, V, String, V> {
    private final String windowStoreName;
    private final Duration retentionWindowDuration;
    private ProcessorContext<String, V> processorContext;
    private WindowStore<String, String> dedupWindowStore;

    /**
     * Constructor.
     *
     * @param windowStoreName The name of the constructor
     * @param retentionWindowDuration The retentionWindow Duration
     */
    public DedupKeyProcessor(String windowStoreName, Duration retentionWindowDuration) {
        this.windowStoreName = windowStoreName;
        this.retentionWindowDuration = retentionWindowDuration;
    }

    /**
     * Initialize the processor.
     *
     * @param context the processor context
     */
    @Override
    public void init(ProcessorContext<String, V> context) {
        processorContext = context;
        dedupWindowStore = processorContext.getStateStore(windowStoreName);
    }

    /**
     * Process a record.
     *
     * @param message the record to process
     */
    @Override
    public void process(Record<String, V> message) {
        Instant currentInstant = Instant.ofEpochMilli(message.timestamp());

        // Retrieve all the matching keys in the state store and return null if found it (signaling a duplicate)
        try (WindowStoreIterator<String> resultIterator = dedupWindowStore.backwardFetch(
                message.key(),
                currentInstant.minus(retentionWindowDuration),
                currentInstant.plus(retentionWindowDuration))) {
            while (resultIterator != null && resultIterator.hasNext()) {
                KeyValue<Long, String> currentKeyValue = resultIterator.next();
                if (message.key().equals(currentKeyValue.value)) {
                    return;
                }
            }
        }

        // First time we see this record, store entry in the window store and forward the record to the output
        dedupWindowStore.put(message.key(), message.key(), message.timestamp());
        processorContext.forward(message);
    }
}
