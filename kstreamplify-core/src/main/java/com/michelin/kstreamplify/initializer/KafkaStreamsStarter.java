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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/** The Kafka Streams starter interface. */
@Slf4j
public abstract class KafkaStreamsStarter {
    /** Constructor. */
    protected KafkaStreamsStarter() {}

    /**
     * The Kafka Streams topology definition.
     *
     * @param streamsBuilder The streams builder
     */
    public abstract void topology(StreamsBuilder streamsBuilder);

    /**
     * The Kafka Streams DLQ topic name.
     *
     * @return The DLQ topic name
     */
    public abstract String dlqTopic();

    /**
     * Runnable code after the Kafka Streams startup.
     *
     * @param kafkaStreams The Kafka Streams instance
     */
    public void onStart(KafkaStreams kafkaStreams) {}

    /**
     * Default uncaught exception handler that shuts down the Kafka Streams client when an uncaught exception occurs.
     *
     * @return The default uncaught exception handler
     */
    public StreamsUncaughtExceptionHandler uncaughtExceptionHandler() {
        return null;
    }
}
