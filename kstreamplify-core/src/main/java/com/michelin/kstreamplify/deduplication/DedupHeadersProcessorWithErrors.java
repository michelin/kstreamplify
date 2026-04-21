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

import com.michelin.kstreamplify.error.ProcessingResult;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Processor class for the deduplication mechanism on headers of a given topic.
 *
 * @param <V> The type of the value
 */
public class DedupHeadersProcessorWithErrors<V extends SpecificRecord>
        implements Processor<String, V, String, ProcessingResult<V, V>> {
    private final String windowStoreName;
    private final Duration retentionWindowDuration;
    private final List<String> deduplicationHeaders;
    private ProcessorContext<String, ProcessingResult<V, V>> processorContext;
    private WindowStore<String, String> dedupWindowStore;

    /**
     * Constructor.
     *
     * @param windowStoreName Name of the deduplication state store
     * @param retentionWindowDuration Retention window duration
     * @param deduplicationHeaders Deduplication headers list
     */
    public DedupHeadersProcessorWithErrors(
            String windowStoreName, Duration retentionWindowDuration, List<String> deduplicationHeaders) {
        this.windowStoreName = windowStoreName;
        this.retentionWindowDuration = retentionWindowDuration;
        this.deduplicationHeaders = deduplicationHeaders;
    }

    /**
     * Initialize the processor.
     *
     * @param context the processor context
     */
    @Override
    public void init(ProcessorContext<String, ProcessingResult<V, V>> context) {
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
        try {
            Instant currentInstant = Instant.ofEpochMilli(message.timestamp());
            String identifier = buildIdentifier(message.headers());

            // Retrieve all the matching keys in the state store and return null if found it (signaling a duplicate)
            try (WindowStoreIterator<String> resultIterator = dedupWindowStore.backwardFetch(
                    identifier,
                    currentInstant.minus(retentionWindowDuration),
                    currentInstant.plus(retentionWindowDuration))) {
                while (resultIterator != null && resultIterator.hasNext()) {
                    KeyValue<Long, String> currentKeyValue = resultIterator.next();
                    if (identifier.equals(currentKeyValue.value)) {
                        return;
                    }
                }
            }
            // First time we see this record, store entry in the window store and forward the record to the output
            dedupWindowStore.put(identifier, identifier, message.timestamp());
            processorContext.forward(ProcessingResult.wrapRecordSuccess(message));
        } catch (Exception e) {
            processorContext.forward(ProcessingResult.wrapRecordFailure(
                    e,
                    message,
                    "Could not figure out what to do with the current payload: "
                            + "An unlikely error occurred during deduplication transform"));
        }
    }

    /**
     * Build an identifier for the record based on the headers and the keys provided.
     *
     * @param headers The headers of the record
     * @return The built identifier
     */
    private String buildIdentifier(Headers headers) {
        return deduplicationHeaders.stream().map(key -> getHeader(headers, key)).collect(Collectors.joining("#"));
    }

    /**
     * Get the header value for a given key
     *
     * @param headers headers of the record
     * @param key the key to look for in the headers
     * @return The header value for the given key, or an empty string if the header is not present or has no value.
     */
    private static String getHeader(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        if (header == null || header.value() == null) {
            return StringUtils.EMPTY;
        }
        String value = new String(header.value(), StandardCharsets.UTF_8);
        return StringUtils.defaultString(value);
    }
}
