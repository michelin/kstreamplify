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
package com.michelin.kstreamplify;

import com.michelin.kstreamplify.configuration.Configuration;
import com.michelin.kstreamplify.configuration.WordCountConfiguration;
import com.michelin.kstreamplify.matchers.OutputTopicContains;
import com.michelin.kstreamplify.topics.PipedOutputTopic;
import com.michelin.kstreamplify.topics.WordCountInputTopic;
import com.michelin.kstreamplify.topics.WordCountOutputTopic;
import com.michelin.kstreamplify.topology.WordCountTopology;
import com.yegor256.Mktmp;
import com.yegor256.MktmpResolver;
import java.nio.file.Path;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.cactoos.map.MapEntry;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MktmpResolver.class)
final class WordCountTopologyTest {

    private TopologyTestDriver driver;

    @BeforeEach
    void setUp(@Mktmp final Path tmp) {
        this.driver = new TopologyTestDriver(
                new WordCountTopology().value(),
                new Configuration.Overridden(
                                new WordCountConfiguration(),
                                new MapEntry<>(StreamsConfig.STATE_DIR_CONFIG, tmp.toString()))
                        .properties());
    }

    @AfterEach
    void tearDown() {
        if (this.driver != null) {
            this.driver.close();
        }
    }

    @Test
    void countWordsIsOk() {
        MatcherAssert.assertThat(
                "When a word count topology is applied to an input topic, the output topic contains the correct word counts",
                new PipedOutputTopic<>(new WordCountInputTopic(this.driver), new WordCountOutputTopic(this.driver))
                        .apply("key1", "Hello Kafka Kafka Streams"),
                new OutputTopicContains<>(
                        new KeyValue<>("hello", 1L),
                        new KeyValue<>("kafka", 1L),
                        new KeyValue<>("kafka", 2L),
                        new KeyValue<>("streams", 1L)));
    }
}
