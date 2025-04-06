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
package com.michelin.kstreamplify.serde;

import com.michelin.kstreamplify.topic.TopicUtils;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * TopicWithSerde API.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public class TopicWithSerde<K, V> {
    /** Default prefix property name. */
    public static final String SELF = "self";

    private final String topicName;
    private final String prefixKey;

    @Getter
    private final Serde<K> keySerde;

    @Getter
    private final Serde<V> valueSerde;

    /**
     * Constructor.
     *
     * @param topicName The name of the topic
     * @param keySerde The key serde
     * @param valueSerde The value serde
     */
    public TopicWithSerde(String topicName, Serde<K> keySerde, Serde<V> valueSerde) {
        this.topicName = topicName;
        this.prefixKey = SELF;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Constructor.
     *
     * @param topicName The name of the topic
     * @param prefixKey The prefix key
     * @param keySerde The key serde
     * @param valueSerde The value serde
     */
    public TopicWithSerde(String topicName, String prefixKey, Serde<K> keySerde, Serde<V> valueSerde) {
        this.topicName = topicName;
        this.prefixKey = prefixKey;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Get the unprefixed topic name.
     *
     * @return The unprefixed topic name
     */
    public String getUnPrefixedName() {
        return topicName;
    }

    /**
     * Remap and prefix the topic name.
     *
     * @return The remapped and prefixed topic name
     */
    @Override
    public String toString() {
        return TopicUtils.remapAndPrefix(topicName, prefixKey);
    }

    /**
     * Wrapper for {@link StreamsBuilder#stream(String, Consumed)}.
     *
     * @param streamsBuilder The streams builder
     * @return A ${@link KStream} from the given topic
     */
    public KStream<K, V> stream(StreamsBuilder streamsBuilder) {
        return streamsBuilder.stream(this.toString(), Consumed.with(keySerde, valueSerde));
    }

    /**
     * Wrapper for {@link StreamsBuilder#table(String, Consumed, Materialized)}.
     *
     * @param streamsBuilder The streams builder
     * @param storeName The store name
     * @return A ${@link KTable} from the given topic
     */
    public KTable<K, V> table(StreamsBuilder streamsBuilder, String storeName) {
        return streamsBuilder.table(
                this.toString(),
                Consumed.with(keySerde, valueSerde),
                Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                        .withKeySerde(keySerde)
                        .withValueSerde(valueSerde));
    }

    /**
     * Wrapper for {@link StreamsBuilder#globalTable(String, Consumed, Materialized)}.
     *
     * @param streamsBuilder The streams builder
     * @param storeName The store name
     * @return A ${@link GlobalKTable} from the given topic
     */
    public GlobalKTable<K, V> globalTable(StreamsBuilder streamsBuilder, String storeName) {
        return streamsBuilder.globalTable(
                this.toString(),
                Consumed.with(keySerde, valueSerde),
                Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                        .withKeySerde(keySerde)
                        .withValueSerde(valueSerde));
    }

    /**
     * Wrapper for {@link KStream#to(String, Produced)}.
     *
     * @param stream The stream to produce
     */
    public void produce(KStream<K, V> stream) {
        stream.to(this.toString(), Produced.with(keySerde, valueSerde));
    }
}
