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
package com.michelin.kstreamplify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.test.TestRecord;

/**
 * A Kafka record with headers, key and value (following the Adapter pattern).
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 *     <p><b>Example:</b>
 *     <pre>{@code
 * // Create a KafkaRecord from a ConsumerRecord
 * ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(
 *     "topic", 0, 0, "key", "value"
 * );
 * KafkaRecord<String, String> record = new KafkaRecord(consumerRecord);
 *
 * // Access headers, key, and value
 * Headers headers = record.headers();
 * String key = record.key();
 * String value = record.value();
 * }</pre>
 */
public final class KafkaRecord<K, V> implements WithHeaders, WithKey<K>, WithValue<V> {

    /** The headers of the record. */
    private final Headers headers;

    /** The key of the record. */
    private final K key;

    /** The value of the record. */
    private final V value;

    /**
     * Primary ctor.
     *
     * @param headers The headers of the record
     * @param key The key of the record
     * @param value The value of the record
     */
    public KafkaRecord(final Headers headers, final K key, final V value) {
        this.headers = headers;
        this.key = key;
        this.value = value;
    }

    /**
     * Secondary ctor, from a {@link TestRecord}.
     *
     * @param record The test record to adapt
     */
    public KafkaRecord(final TestRecord<K, V> record) {
        this(record.headers(), record.key(), record.value());
    }

    /**
     * Secondary ctor, from a {@link ConsumerRecord}.
     *
     * @param record The consumer record to adapt
     */
    public KafkaRecord(final ConsumerRecord<K, V> record) {
        this(record.headers(), record.key(), record.value());
    }

    @Override
    public Headers headers() {
        return this.headers;
    }

    @Override
    public K key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public String toString() {
        return String.format("[headers=%s, key=%s, value=%s]", this.headers, this.key, this.value);
    }
}
