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

import static java.util.Optional.ofNullable;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.property.KafkaProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

/** The Kafka Streams initializer. */
@Slf4j
@Component
@ConditionalOnBean(KafkaStreamsStarter.class)
public class SpringBootKafkaStreamsInitializer extends KafkaStreamsInitializer implements ApplicationRunner {
    private final ConfigurableApplicationContext applicationContext;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param kafkaStreamsStarter The Kafka Streams starter
     * @param serverPort The server port
     * @param kafkaProperties The Spring Boot Kafka properties
     * @param applicationContext The application context
     * @param registry The Micrometer registry
     */
    public SpringBootKafkaStreamsInitializer(
            KafkaStreamsStarter kafkaStreamsStarter,
            @Value("${server.port:8080}") int serverPort,
            KafkaProperties kafkaProperties,
            ConfigurableApplicationContext applicationContext,
            MeterRegistry registry) {
        super(kafkaStreamsStarter, serverPort, kafkaProperties.asProperties());
        this.applicationContext = applicationContext;
        this.registry = registry;
    }

    /**
     * Automatically start Kstreamplify when the Spring Boot application is ready.
     *
     * @param args The program arguments
     */
    @Override
    public void run(ApplicationArguments args) {
        start();
    }

    /**
     * Start Kstreamplify.
     *
     * <ol>
     *   <li>Build the topology.
     *   <li>Create the Kafka Streams instance.
     *   <li>Register Kafka Streams metrics to the Spring Boot registry for OpenTelemetry.
     *   <li>Start the HTTP server.
     * </ol>
     */
    @Override
    public void start() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        kafkaStreamsStarter.topology(streamsBuilder);

        topology = streamsBuilder.build(KafkaStreamsExecutionContext.getProperties());
        log.info("Topology description:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, KafkaStreamsExecutionContext.getProperties());

        KafkaStreamsMetrics kafkaStreamsMetrics = new KafkaStreamsMetrics(kafkaStreams);
        kafkaStreamsMetrics.bindTo(registry);

        kafkaStreamsStarter.onStart(kafkaStreams);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.setUncaughtExceptionHandler(
                ofNullable(kafkaStreamsStarter.uncaughtExceptionHandler()).orElse(this::uncaughtExceptionHandler));

        kafkaStreams.setStateListener(this::stateListener);

        kafkaStreams.start();
    }

    /**
     * Default uncaught exception handler.
     *
     * @param exception The exception
     * @return The uncaught exception response
     */
    @Override
    protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtExceptionHandler(
            Throwable exception) {
        if (applicationContext != null) {
            applicationContext.close();
        }

        return super.uncaughtExceptionHandler(exception);
    }

    /**
     * Default state listener.
     *
     * @param newState The new state
     * @param oldState The old state
     */
    @Override
    protected void stateListener(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState.equals(KafkaStreams.State.ERROR) && applicationContext != null) {
            applicationContext.close();
        }
    }
}
