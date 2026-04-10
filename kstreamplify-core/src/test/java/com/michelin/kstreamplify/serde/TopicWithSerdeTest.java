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

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kstreamplify.avro.KafkaUserStub;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.Test;

class TopicWithSerdeTest {

    @Test
    void shouldCreateTopicWithSerde() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        assertEquals("INPUT_TOPIC", topicWithSerde.getUnPrefixedName());
        assertEquals("INPUT_TOPIC", topicWithSerde.toString());
    }

    @Test
    void shouldCreateTopicWithSerdeWithPrefix() {
        Properties properties = new Properties();
        properties.put("prefix.self", "abc.");

        KafkaStreamsExecutionContext.registerProperties(properties);

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        assertEquals("INPUT_TOPIC", topicWithSerde.getUnPrefixedName());
        assertEquals("abc.INPUT_TOPIC", topicWithSerde.toString());
    }

    @Test
    void shouldCreateStream() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topicWithSerde.stream(streamsBuilder);

        assertEquals("""
                Topologies:
                   Sub-topology: 0
                    Source: KSTREAM-SOURCE-0000000000 (topics: [INPUT_TOPIC])
                      --> none

                """, streamsBuilder.build().describe().toString());
    }

    @Test
    void shouldCreateStreamWithName() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topicWithSerde.stream(streamsBuilder, "consumer-name");
        assertEquals("""
                Topologies:
                   Sub-topology: 0
                    Source: consumer-name (topics: [INPUT_TOPIC])
                      --> none

                """, streamsBuilder.build().describe().toString());
    }

    @Test
    void shouldCreateTable() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topicWithSerde.table(streamsBuilder, "myStore");

        assertEquals("""
                Topologies:
                   Sub-topology: 0
                    Source: KSTREAM-SOURCE-0000000000 (topics: [INPUT_TOPIC])
                      --> KTABLE-SOURCE-0000000001
                    Processor: KTABLE-SOURCE-0000000001 (stores: [myStore])
                      --> none
                      <-- KSTREAM-SOURCE-0000000000

                """, streamsBuilder.build().describe().toString());
    }

    @Test
    void shouldCreateTableWithName() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topicWithSerde.table(streamsBuilder, "myStore", "table-processor-name");

        assertEquals("""
                Topologies:
                   Sub-topology: 0
                    Source: table-processor-name-source (topics: [INPUT_TOPIC])
                      --> table-processor-name
                    Processor: table-processor-name (stores: [myStore])
                      --> none
                      <-- table-processor-name-source

                """, streamsBuilder.build().describe().toString());
    }

    @Test
    void shouldCreateGlobalKtable() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topicWithSerde.globalTable(streamsBuilder, "myStore");

        assertEquals("""
                Topologies:
                   Sub-topology: 0 for global store (will not generate tasks)
                    Source: KSTREAM-SOURCE-0000000000 (topics: [INPUT_TOPIC])
                      --> KTABLE-SOURCE-0000000001
                    Processor: KTABLE-SOURCE-0000000001 (stores: [myStore])
                      --> none
                      <-- KSTREAM-SOURCE-0000000000
                """, streamsBuilder.build().describe().toString());
    }

    @Test
    void shouldCreateGlobalKtableWithName() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topicWithSerde.globalTable(streamsBuilder, "myStore", "global-table-processor-name");
        System.out.println(streamsBuilder.build().describe());

        assertEquals("""
                Topologies:
                   Sub-topology: 0 for global store (will not generate tasks)
                    Source: global-table-processor-name-source (topics: [INPUT_TOPIC])
                      --> global-table-processor-name
                    Processor: global-table-processor-name (stores: [myStore])
                      --> none
                      <-- global-table-processor-name-source
                """, streamsBuilder.build().describe().toString());
    }

    @Test
    void shouldCreateGlobalStore() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());
        KafkaStreamsExecutionContext.setSerdesConfig(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"));

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        TopicWithSerde<String, KafkaUserStub> topicUserWithSerde =
                new TopicWithSerde<>("INPUT_USER_TOPIC", Serdes.String(), SerdesUtils.getValueSerdes());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("myStore"), Serdes.String(), Serdes.String());

        StoreBuilder<TimestampedKeyValueStore<String, KafkaUserStub>> timestampedStoreBuilder =
                Stores.timestampedKeyValueStoreBuilder(
                        Stores.persistentTimestampedKeyValueStore("myTimestampedStore"),
                        Serdes.String(),
                        SerdesUtils.getValueSerdes());

        topicWithSerde.addGlobalStore(streamsBuilder, storeBuilder, MyStringProcessor::new);
        topicUserWithSerde.addGlobalStore(streamsBuilder, timestampedStoreBuilder, MyUserProcessor::new);

        assertEquals("""
                Topologies:
                   Sub-topology: 0 for global store (will not generate tasks)
                    Source: KSTREAM-SOURCE-0000000000 (topics: [INPUT_TOPIC])
                      --> KTABLE-SOURCE-0000000001
                    Processor: KTABLE-SOURCE-0000000001 (stores: [myStore])
                      --> none
                      <-- KSTREAM-SOURCE-0000000000
                  Sub-topology: 1 for global store (will not generate tasks)
                    Source: KSTREAM-SOURCE-0000000002 (topics: [INPUT_USER_TOPIC])
                      --> KTABLE-SOURCE-0000000003
                    Processor: KTABLE-SOURCE-0000000003 (stores: [myTimestampedStore])
                      --> none
                      <-- KSTREAM-SOURCE-0000000002
                """, streamsBuilder.build().describe().toString());
    }

    @Test
    void shouldCreateGlobalStoreWithName() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());
        KafkaStreamsExecutionContext.setSerdesConfig(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://mock:8081"));

        TopicWithSerde<String, String> topicWithSerde =
                new TopicWithSerde<>("INPUT_TOPIC", Serdes.String(), Serdes.String());

        TopicWithSerde<String, KafkaUserStub> topicUserWithSerde =
                new TopicWithSerde<>("INPUT_USER_TOPIC", Serdes.String(), SerdesUtils.getValueSerdes());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("myStore"), Serdes.String(), Serdes.String());

        StoreBuilder<TimestampedKeyValueStore<String, KafkaUserStub>> timestampedStoreBuilder =
                Stores.timestampedKeyValueStoreBuilder(
                        Stores.persistentTimestampedKeyValueStore("myTimestampedStore"),
                        Serdes.String(),
                        SerdesUtils.getValueSerdes());

        topicWithSerde.addGlobalStore(
                streamsBuilder, storeBuilder, MyStringProcessor::new, "kv-global-store-processor");
        topicUserWithSerde.addGlobalStore(
                streamsBuilder, timestampedStoreBuilder, MyUserProcessor::new, "timestamped-kv-global-store-processor");
        System.out.println(streamsBuilder.build().describe());
        assertEquals("""
                Topologies:
                   Sub-topology: 0 for global store (will not generate tasks)
                    Source: kv-global-store-processor-source (topics: [INPUT_TOPIC])
                      --> kv-global-store-processor
                    Processor: kv-global-store-processor (stores: [myStore])
                      --> none
                      <-- kv-global-store-processor-source
                  Sub-topology: 1 for global store (will not generate tasks)
                    Source: timestamped-kv-global-store-processor-source (topics: [INPUT_USER_TOPIC])
                      --> timestamped-kv-global-store-processor
                    Processor: timestamped-kv-global-store-processor (stores: [myTimestampedStore])
                      --> none
                      <-- timestamped-kv-global-store-processor-source
                """, streamsBuilder.build().describe().toString());
    }

    private static class MyStringProcessor extends ContextualProcessor<String, String, Void, Void> {
        private KeyValueStore<String, String> store;

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            this.store = context.getStateStore("users-store");
        }

        @Override
        public void process(Record<String, String> record) {
            store.put(record.key(), record.value());
        }
    }

    private static class MyUserProcessor extends ContextualProcessor<String, KafkaUserStub, Void, Void> {
        private TimestampedKeyValueStore<String, KafkaUserStub> store;

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            this.store = context.getStateStore("users-store");
        }

        @Override
        public void process(Record<String, KafkaUserStub> record) {
            store.put(record.key(), ValueAndTimestamp.make(record.value(), record.timestamp()));
        }
    }
}
