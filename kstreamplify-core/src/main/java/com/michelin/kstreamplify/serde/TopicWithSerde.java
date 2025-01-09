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
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
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
 * Wrapper class for simplifying topics interactions and their behaviors.
 *
 * @param <K> The model used as the key avro of the topic. Can be String (Recommended)
 * @param <V> The model used as the value avro of the topic.
 */
@AllArgsConstructor(access = AccessLevel.PUBLIC)
public class TopicWithSerde<K, V> {
    /**
     * Default prefix property name.
     */
    public static final String SELF = "self";

    /**
     * Name of the topic.
     */
    private final String topicName;

    /**
     * Name of the property key defined under kafka.properties.prefix.
     * Used to prefix the topicName dynamically at runtime.
     * For instance, with the given following configuration:
     * <pre>{@code
     * kafka:
     *   properties:
     *     prefix:
     *       nsKey: "myNamespacePrefix."
     * }</pre>
     * If the topic name is {@code myTopic}, at stream initialization the topic name wil resolve
     * to {@code myNamespacePrefix.myTopic}.
     */
    private final String prefixPropertyKey;

    /**
     * Key serde for the topic.
     */
    @Getter
    private final Serde<K> keySerde;

    /**
     * Value serde for the topic.
     */
    @Getter
    private final Serde<V> valueSerde;

    /**
     * Additional constructor which uses default parameter "self" for prefixPropertyKey.
     * For instance, with the given following configuration:
     * <pre>{@code
     * kafka:
     *   properties:
     *     prefix:
     *       self: "myNamespacePrefix."
     * }</pre>
     * If the topic name is {@code myTopic}, at stream initialization the topic name wil resolve
     * to {@code myNamespacePrefix.myTopic}.
     *
     * @param topicName  Name of the topic
     * @param keySerde   Key serde for the topic
     * @param valueSerde Value serde for the topic
     */
    public TopicWithSerde(String topicName, Serde<K> keySerde, Serde<V> valueSerde) {
        this.topicName = topicName;
        this.prefixPropertyKey = SELF;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Get the un-prefixed name of the Topic for specific usage.
     *
     * @return The name of the topic, as defined during initialization
     */
    public String getUnPrefixedName() {
        return topicName;
    }

    /**
     * Override of the toString method, dynamically builds the topicName based on springBoot
     * properties for environment/application.
     *
     * @return The prefixed name of the topic
     */
    @Override
    public String toString() {
        return TopicUtils.remapAndPrefix(topicName, prefixPropertyKey);
    }

    /**
     * Wrapper for the .stream method of KafkaStreams.
     * Allows simple usage of a topic with type inference
     *
     * @param sb The streamsBuilder
     * @return a Kstream from the given topic
     */
    public KStream<K, V> stream(StreamsBuilder sb) {
        return sb.stream(this.toString(), Consumed.with(keySerde, valueSerde));
    }

    /**
     * Wrapper for the .table method of KafkaStreams. Allows simple usage of a topic with type inference
     *
     * @param sb        The streamsBuilder
     * @param storeName The StoreName
     * @return a KTable from the given topic
     */
    public KTable<K, V> table(StreamsBuilder sb, String storeName) {
        return sb.table(this.toString(), Consumed.with(keySerde, valueSerde),
            Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName).withKeySerde(keySerde)
                .withValueSerde(valueSerde));
    }

    /**
     * Wrapper for the .globalTable method of KafkaStreams. Allows simple usage of a topic with type inference
     *
     * @param sb        The streamsBuilder
     * @param storeName The StoreName
     * @return a GlobalKTable from the given topic
     */
    public GlobalKTable<K, V> globalTable(StreamsBuilder sb, String storeName) {
        return sb.globalTable(this.toString(), Consumed.with(keySerde, valueSerde),
            Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName).withKeySerde(keySerde)
                .withValueSerde(valueSerde));
    }

    /**
     * Wrapper for the .to method of Kafka streams. Allows simple usage of a topic with type inference
     *
     * @param stream The stream to produce in the topic
     */
    public void produce(KStream<K, V> stream) {
        stream.to(this.toString(), Produced.with(keySerde, valueSerde));
    }
}
