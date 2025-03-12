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
package com.michelin.kstreamplify.initializer;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.deduplication.DeduplicationUtils;
import com.michelin.kstreamplify.error.ProcessingResult;
import com.michelin.kstreamplify.error.TopologyErrorHandler;
import com.michelin.kstreamplify.serde.SerdesUtils;
import com.michelin.kstreamplify.serde.TopicWithSerde;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.Test;

class KafkaStreamsStarterTest {
    @Test
    void shouldInstantiateKafkaStreamsStarter() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());
        KafkaStreamsExecutionContext.setSerdesConfig(
                Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"));

        StreamsBuilder builder = new StreamsBuilder();
        KafkaStreamsStarterStub starter = new KafkaStreamsStarterStub();
        starter.topology(builder);

        assertNotNull(builder.build().describe());
        assertEquals("DLQ_TOPIC", starter.dlqTopic());

        starter.onStart(null);
        assertTrue(starter.isStarted());
    }

    @Test
    void shouldStartWithCustomUncaughtExceptionHandler() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());
        KafkaStreamsExecutionContext.setSerdesConfig(
                Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"));

        StreamsBuilder builder = new StreamsBuilder();
        KafkaStreamsStarterStub starter = new KafkaStreamsStarterStub();
        starter.topology(builder);

        assertNotNull(builder.build().describe());
        assertEquals("DLQ_TOPIC", starter.dlqTopic());
        assertEquals(
                starter.uncaughtExceptionHandler()
                        .handle(new Exception("Register a custom uncaught exception handler test.")),
                REPLACE_THREAD);

        starter.onStart(null);
        assertTrue(starter.isStarted());
    }

    /** Kafka Streams Starter implementation used for unit tests purpose. */
    @Getter
    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        private boolean started;

        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            var streams = TopicWithSerdeStub.inputTopicWithSerde().stream(streamsBuilder);

            DeduplicationUtils.deduplicateKeys(
                    streamsBuilder,
                    streams,
                    "deduplicateKeysStoreName",
                    "deduplicateKeysRepartitionName",
                    Duration.ZERO);
            DeduplicationUtils.deduplicateKeyValues(
                    streamsBuilder,
                    streams,
                    "deduplicateKeyValuesStoreName",
                    "deduplicateKeyValuesRepartitionName",
                    Duration.ZERO);
            DeduplicationUtils.deduplicateWithPredicate(streamsBuilder, streams, Duration.ofMillis(1), null);

            var enrichedStreams = streams.mapValues(KafkaStreamsStarterStub::enrichValue);
            var enrichedStreams2 = streams.mapValues(KafkaStreamsStarterStub::enrichValue2);
            var processingResults = TopologyErrorHandler.catchErrors(enrichedStreams);
            TopologyErrorHandler.catchErrors(enrichedStreams2, true);
            TopicWithSerdeStub.outputTopicWithSerde().produce(processingResults);
        }

        @Override
        public String dlqTopic() {
            return "DLQ_TOPIC";
        }

        @Override
        public void onStart(KafkaStreams kafkaStreams) {
            started = true;
        }

        @Override
        public StreamsUncaughtExceptionHandler uncaughtExceptionHandler() {
            return new UncaughtExceptionHandlerStub();
        }

        private static ProcessingResult<String, String> enrichValue(KafkaError input) {
            if (input != null) {
                return ProcessingResult.success("output field");
            } else {
                return ProcessingResult.fail(new IOException("an exception occurred"), "output error");
            }
        }

        private static ProcessingResult<String, String> enrichValue2(KafkaError input) {
            if (input != null) {
                return ProcessingResult.success("output field 2");
            } else {
                return ProcessingResult.fail(new IOException("an exception occurred"), "output error 2");
            }
        }
    }

    /**
     * Topic with serdes helper used for unit tests purpose.
     *
     * @param <K> The key type
     * @param <V> The value type
     */
    static class TopicWithSerdeStub<K, V> extends TopicWithSerde<K, V> {
        private TopicWithSerdeStub(String name, String appName, Serde<K> keySerde, Serde<V> valueSerde) {
            super(name, appName, keySerde, valueSerde);
        }

        public static TopicWithSerdeStub<String, String> outputTopicWithSerde() {
            return new TopicWithSerdeStub<>("OUTPUT_TOPIC", "APP_NAME", Serdes.String(), Serdes.String());
        }

        public static TopicWithSerdeStub<String, KafkaError> inputTopicWithSerde() {
            return new TopicWithSerdeStub<>("INPUT_TOPIC", "APP_NAME", Serdes.String(), SerdesUtils.getValueSerdes());
        }
    }

    @Slf4j
    static class UncaughtExceptionHandlerStub implements StreamsUncaughtExceptionHandler {
        @Override
        public StreamThreadExceptionResponse handle(final Throwable t) {
            log.error("!Custom uncaught exception handler test!");
            return REPLACE_THREAD;
        }
    }
}
