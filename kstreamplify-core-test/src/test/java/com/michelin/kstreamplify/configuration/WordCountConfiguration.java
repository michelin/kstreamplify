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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.cactoos.map.MapEntry;

/** A configuration for the WordCount topology in tests. */
public final class WordCountConfiguration extends Configuration.Envelope {
    /** Constructs a WordCountConfiguration with the given state directory. */
    public WordCountConfiguration() {
        super(new Configuration.FromMap(
                new MapEntry<>(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-test"),
                new MapEntry<>(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
                new MapEntry<>(
                        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                        Serdes.String().getClass().getName()),
                new MapEntry<>(
                        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                        Serdes.String().getClass().getName())));
    }
}
