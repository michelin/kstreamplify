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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/** The Kafka Streams starter interface. */
public abstract class KafkaStreamsStarter {

    /** Constructor. */
    protected KafkaStreamsStarter() {}

    /**
     * Define the topology of the Kafka Streams.
     *
     * @param streamsBuilder The streams builder
     */
    public abstract void topology(StreamsBuilder streamsBuilder);

    /**
     * Define the dead letter queue (DLQ) topic. If you don't want to use the DLQ topic, you can return
     * {@link org.apache.commons.lang3.StringUtils#EMPTY}.
     *
     * @return The dead letter queue (DLQ) topic
     */
    public abstract String dlqTopic();

    /**
     * Define runnable code after the Kafka Streams startup.
     *
     * @param kafkaStreams The Kafka Streams instance
     */
    public void onStart(KafkaStreams kafkaStreams) {}

    /**
     * Register a custom uncaught exception handler.
     *
     * @return StreamsUncaughtExceptionHandler The custom uncaught exception handler
     */
    public StreamsUncaughtExceptionHandler uncaughtExceptionHandler() {
        return null;
    }
}
