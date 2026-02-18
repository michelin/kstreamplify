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

import com.michelin.kstreamplify.configuration.ItConsumerConfiguration;
import com.michelin.kstreamplify.configuration.ItProducerConfiguration;
import com.michelin.kstreamplify.configuration.ItStreamsConfiguration;
import com.michelin.kstreamplify.matchers.ConsumerPolls;
import com.michelin.kstreamplify.topology.WordCountTopology;
import com.yegor256.MayBeSlow;
import com.yegor256.Mktmp;
import com.yegor256.MktmpResolver;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.cactoos.map.MapEntry;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(MktmpResolver.class)
final class WordCountIntegrationTest {

    private ConfluentKafkaContainer kafka;

    @BeforeEach
    void setUp() {
        this.kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
        kafka.start();
    }

    @AfterEach
    void tearDown() {
        if (this.kafka != null) {
            this.kafka.stop();
        }
    }

    @Test
    @ExtendWith(MayBeSlow.class)
    void shouldProduceFinalWordCounts(@Mktmp final Path tmp) throws Exception {
        try (KafkaStreams streams = new KafkaStreams(
                new WordCountTopology().value(), new ItStreamsConfiguration(this.kafka, tmp).properties())) {
            streams.start();
            try (Producer<String, String> producer =
                    new KafkaProducer<>(new ItProducerConfiguration(this.kafka).properties())) {
                producer.send(new ProducerRecord<>("input-topic", "key1", "Hello Kafka Kafka Streams"))
                        .get();
            }
            try (Consumer<String, Long> consumer =
                    new KafkaConsumer<>(new ItConsumerConfiguration(this.kafka).properties())) {
                MatcherAssert.assertThat(
                        "Output topic should contain the expected word counts in order",
                        consumer,
                        new ConsumerPolls<>(
                                Collections.singletonList("output-topic"),
                                new MapEntry<>("hello", 1L),
                                new MapEntry<>("kafka", 2L),
                                new MapEntry<>("streams", 1L)));
            } finally {
                streams.close(Duration.ofSeconds(5));
            }
        }
    }
}
