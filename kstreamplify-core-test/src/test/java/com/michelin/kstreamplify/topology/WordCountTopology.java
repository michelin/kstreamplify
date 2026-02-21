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
package com.michelin.kstreamplify.topology;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.cactoos.Scalar;

/** A simple WordCount topology for testing purposes. */
public final class WordCountTopology implements Scalar<Topology> {
    @Override
    public Topology value() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> source =
                builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()));
        final KTable<String, Long> counts = source.flatMapValues(
                        value -> Arrays.stream(value.toLowerCase().split("\\W+"))
                                .filter(s -> !s.isEmpty())
                                .collect(Collectors.toList()))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));
        counts.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }
}
