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
package com.michelin.kstreamplify.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopologyServiceTest {
    @Mock
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    @InjectMocks
    private TopologyService topologyService;

    @Test
    void shouldExposeTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsStarter starter = new KafkaStreamsStarterStub();
        starter.topology(streamsBuilder);

        when(kafkaStreamsInitializer.getTopology()).thenReturn(streamsBuilder.build());

        String response = topologyService.getTopology();

        assertEquals("""
            Topologies:
               Sub-topology: 0
                Source: KSTREAM-SOURCE-0000000000 (topics: [INPUT_TOPIC])
                  --> KSTREAM-SINK-0000000001
                Sink: KSTREAM-SINK-0000000001 (topic: OUTPUT_TOPIC)
                  <-- KSTREAM-SOURCE-0000000000

            """, response);
    }

    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder.stream("INPUT_TOPIC").to("OUTPUT_TOPIC");
        }

        @Override
        public String dlqTopic() {
            return "DLQ_TOPIC";
        }
    }
}
