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
package com.michelin.kstreamplify.configuration;

import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.cactoos.map.MapEntry;

/** A {@link Configuration} for Kafka Streams in integration tests. */
public final class ItStreamsConfiguration extends Configuration.Envelope {
    /** Constructs an ItStreamsConfiguration with the given Kafka container and temporary directory. */
    public ItStreamsConfiguration() {
        super(new Configuration.FromMap(
                new MapEntry<>(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-it-" + UUID.randomUUID()),
                new MapEntry<>(
                        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                        Serdes.String().getClass().getName()),
                new MapEntry<>(
                        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                        Serdes.String().getClass().getName())));
    }
}
