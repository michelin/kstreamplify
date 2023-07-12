package com.michelin.kstreamplify.utils;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Wrapper class for simplifying topics interactions and their behaviors
 *
 * @param <K> The model used as the key avro of the topic. Can be String (Recommended)
 * @param <V> The model used as the value avro of the topic.
 */
public abstract class TopicWithSerde<K, V> {

    /**
     * Name of the topic
     */
    private final String topicName;

    /**
     * Owner application of the topic
     */
    private final String appName;

    /**
     * Key serde
     */
    private final Serde<K> keySerde;

    public Serde<K> getKeySerde() {
        return keySerde;
    }

    /**
     * Value serde
     */
    private final Serde<V> valueSerde;

    public Serde<V> getValueSerde() {
        return valueSerde;
    }

    /**
     * Public constructor
     *
     * @param topicName  Name of the topic
     * @param appName    Owner application of the topic. Must be used in pair with springboot configuration topic.prefix.[appName]
     * @param keySerde   Key serde for the topic
     * @param valueSerde Value serde for the topic
     */
    protected TopicWithSerde(String topicName, String appName, Serde<K> keySerde, Serde<V> valueSerde) {
        this.topicName = topicName;
        this.appName = appName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    /**
     * Get the un-prefixed name of the Topic for specific usage
     *
     * @return The name of the topic, as defined during initialization, without ns4kafka prefixing
     */
    public String getUnPrefixedName() {
        return topicName;
    }

    /**
     * Override of the toString method, dynamically builds the topicName based on springBoot properties for environment/application
     *
     * @return The prefixed name of the topic
     */
    @Override
    public String toString() {
        return TopicUtils.prefixAndDynamicRemap(topicName, appName);
    }

    /**
     * Wrapper for the .stream method of KafkaStreams. Allows simple usage of a topic with type inference
     *
     * @param sb
     * @return
     */
    public KStream<K, V> stream(StreamsBuilder sb) {
        return sb.stream(this.toString(), Consumed.with(keySerde, valueSerde));
    }

    /**
     * Wrapper for the .table method of KafkaStreams. Allows simple usage of a topic with type inference
     *
     * @param sb        The streamsBuilder
     * @param storeName The StoreName
     * @return a Ktable from the given topic
     */
    public KTable<K, V> table(StreamsBuilder sb, String storeName) {
        return sb.table(this.toString(), Consumed.with(keySerde, valueSerde), Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName).withKeySerde(keySerde).withValueSerde(valueSerde));
    }

    /**
     * Wrapper for the .globalTable method of KafkaStreams. Allows simple usage of a topic with type inference
     *
     * @param sb        The streamsBuilder
     * @param storeName The StoreName
     * @return a GlobalKtable from the given topic
     */
    public GlobalKTable<K, V> globalTable(StreamsBuilder sb, String storeName) {
        return sb.globalTable(this.toString(), Consumed.with(keySerde, valueSerde), Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName).withKeySerde(keySerde).withValueSerde(valueSerde));
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
