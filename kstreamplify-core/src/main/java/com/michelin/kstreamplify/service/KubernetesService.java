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

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import java.net.HttpURLConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.StreamThread;

/** Kafka Streams Kubernetes service. */
@Slf4j
public final class KubernetesService {
    /** The readiness path property name. */
    public static final String READINESS_PATH_PROPERTY_NAME = "kubernetes.readiness.path";

    /** The liveness path property name. */
    public static final String LIVENESS_PATH_PROPERTY_NAME = "kubernetes.liveness.path";

    /** The default readiness path. */
    public static final String DEFAULT_READINESS_PATH = "ready";

    /** The default liveness path. */
    public static final String DEFAULT_LIVENESS_PATH = "liveness";

    private final KafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    public KubernetesService(KafkaStreamsInitializer kafkaStreamsInitializer) {
        this.kafkaStreamsInitializer = kafkaStreamsInitializer;
    }

    /**
     * Kubernetes' readiness probe.
     *
     * @return An HTTP response code
     */
    public int getReadiness() {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            log.debug(
                    "Kafka Stream \"{}\" state: {}",
                    KafkaStreamsExecutionContext.getProperties().getProperty(StreamsConfig.APPLICATION_ID_CONFIG),
                    kafkaStreamsInitializer.getKafkaStreams().state());

            if (kafkaStreamsInitializer.getKafkaStreams().state() == KafkaStreams.State.REBALANCING) {
                long startingThreadCount = kafkaStreamsInitializer.getKafkaStreams().metadataForLocalThreads().stream()
                        .filter(t -> StreamThread.State.STARTING.name().compareToIgnoreCase(t.threadState()) == 0
                                || StreamThread.State.CREATED.name().compareToIgnoreCase(t.threadState()) == 0)
                        .count();

                if (startingThreadCount
                        == kafkaStreamsInitializer
                                .getKafkaStreams()
                                .metadataForLocalThreads()
                                .size()) {
                    return HttpURLConnection.HTTP_NO_CONTENT;
                }
            }

            return kafkaStreamsInitializer.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)
                    ? HttpURLConnection.HTTP_OK
                    : HttpURLConnection.HTTP_UNAVAILABLE;
        }
        return HttpURLConnection.HTTP_BAD_REQUEST;
    }

    /**
     * Kubernetes' liveness probe.
     *
     * @return An HTTP response code
     */
    public int getLiveness() {
        if (kafkaStreamsInitializer.getKafkaStreams() != null) {
            return kafkaStreamsInitializer.getKafkaStreams().state() != KafkaStreams.State.NOT_RUNNING
                    ? HttpURLConnection.HTTP_OK
                    : HttpURLConnection.HTTP_INTERNAL_ERROR;
        }
        return HttpURLConnection.HTTP_NO_CONTENT;
    }
}
