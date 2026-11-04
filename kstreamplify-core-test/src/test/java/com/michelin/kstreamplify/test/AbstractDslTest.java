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
package com.michelin.kstreamplify.test;

import com.michelin.kstreamplify.KafkaStreamsStarterTest;
import com.michelin.kstreamplify.error.ProcessingResult;
import com.michelin.kstreamplify.error.TopologyErrorHandler;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.serde.TopicWithSerde;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Base class of the testing DSL tests. It provides a topology covering the scenarios exercised by the DSL: multiple
 * input topics, error handling to the DLQ, record headers, a stream-table join, a state store and a wall-clock-time
 * punctuator.
 */
abstract class AbstractDslTest extends KafkaStreamsStarterTest {
    protected static final String DLQ_TOPIC = "DLQ_TOPIC";
    protected static final String USER_STORE = "user-store";
    protected static final String CORRELATION_ID = "correlation-id";
    protected static final String INVALID_VALUE = "boom";
    protected static final Instant INITIAL_TIME = Instant.parse("2020-01-01T00:00:00Z");

    protected static final TopicWithSerde<String, String> INPUT =
            new TopicWithSerde<>("INPUT", Serdes.String(), Serdes.String());
    protected static final TopicWithSerde<String, String> INPUT_2 =
            new TopicWithSerde<>("INPUT_2", Serdes.String(), Serdes.String());
    protected static final TopicWithSerde<String, String> OUTPUT =
            new TopicWithSerde<>("OUTPUT", Serdes.String(), Serdes.String());
    protected static final TopicWithSerde<String, String> USER_TOPIC =
            new TopicWithSerde<>("USER_TOPIC", Serdes.String(), Serdes.String());
    protected static final TopicWithSerde<String, String> ORDER_TOPIC =
            new TopicWithSerde<>("ORDER_TOPIC", Serdes.String(), Serdes.String());
    protected static final TopicWithSerde<String, String> JOIN_OUTPUT =
            new TopicWithSerde<>("JOIN_OUTPUT", Serdes.String(), Serdes.String());
    protected static final TopicWithSerde<String, String> TICK_TOPIC =
            new TopicWithSerde<>("TICK_TOPIC", Serdes.String(), Serdes.String());
    protected static final TopicWithSerde<String, String> TICK_OUTPUT =
            new TopicWithSerde<>("TICK_OUTPUT", Serdes.String(), Serdes.String());

    @Override
    protected Instant getInitialWallClockTime() {
        return INITIAL_TIME;
    }

    @Override
    protected KafkaStreamsStarter getKafkaStreamsStarter() {
        return new KafkaStreamsStarter() {
            @Override
            public String dlqTopic() {
                return DLQ_TOPIC;
            }

            @Override
            public void topology(StreamsBuilder streamsBuilder) {
                buildUpperCaseTopology(streamsBuilder);
                buildJoinTopology(streamsBuilder);
                buildPunctuationTopology(streamsBuilder);
            }
        };
    }

    private void buildUpperCaseTopology(StreamsBuilder streamsBuilder) {
        KStream<String, String> merged = INPUT.stream(streamsBuilder).merge(INPUT_2.stream(streamsBuilder));

        KStream<String, ProcessingResult<String, String>> processed = merged.process(headerAdder())
                .mapValues(value -> {
                    if (INVALID_VALUE.equals(value)) {
                        return ProcessingResult.fail(new IllegalStateException("Invalid value"), value);
                    }
                    return ProcessingResult.success(value.toUpperCase());
                });

        OUTPUT.produce(TopologyErrorHandler.catchErrors(processed));
    }

    private void buildJoinTopology(StreamsBuilder streamsBuilder) {
        KTable<String, String> users = USER_TOPIC.table(streamsBuilder, USER_STORE);

        JOIN_OUTPUT.produce(ORDER_TOPIC.stream(streamsBuilder)
                .join(
                        users,
                        (order, user) -> order + "-" + user,
                        Joined.with(Serdes.String(), Serdes.String(), Serdes.String())));
    }

    private void buildPunctuationTopology(StreamsBuilder streamsBuilder) {
        TICK_OUTPUT.produce(TICK_TOPIC.stream(streamsBuilder).process(tickProcessor()));
    }

    private static ProcessorSupplier<String, String, String, String> headerAdder() {
        return () -> new ContextualProcessor<>() {
            @Override
            public void process(Record<String, String> record) {
                context()
                        .forward(record.withHeaders(record.headers()
                                .add(CORRELATION_ID, record.key().getBytes(StandardCharsets.UTF_8))));
            }
        };
    }

    private static ProcessorSupplier<String, String, String, String> tickProcessor() {
        return () -> new ContextualProcessor<>() {
            @Override
            public void init(ProcessorContext<String, String> context) {
                super.init(context);
                context.schedule(
                        Duration.ofMinutes(1),
                        PunctuationType.WALL_CLOCK_TIME,
                        timestamp -> context.forward(new Record<>("tick", "tick", timestamp)));
            }

            @Override
            public void process(Record<String, String> record) {
                context().forward(record);
            }
        };
    }
}
