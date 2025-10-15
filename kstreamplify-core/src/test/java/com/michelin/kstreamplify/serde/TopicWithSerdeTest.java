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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
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
}
