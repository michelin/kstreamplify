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

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.property.KafkaProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
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
    private final KafkaProperties springBootKafkaProperties;

    @Value("${server.port:8080}")
    private int springBootServerPort;

    /**
     * Constructor.
     *
     * @param applicationContext The application context
     * @param kafkaStreamsStarter The Kafka Streams starter
     * @param springBootKafkaProperties The Spring Boot Kafka properties
     * @param registry The Micrometer registry
     */
    public SpringBootKafkaStreamsInitializer(
            ConfigurableApplicationContext applicationContext,
            KafkaStreamsStarter kafkaStreamsStarter,
            KafkaProperties springBootKafkaProperties,
            MeterRegistry registry) {
        this.applicationContext = applicationContext;
        this.kafkaStreamsStarter = kafkaStreamsStarter;
        this.springBootKafkaProperties = springBootKafkaProperties;
        this.registry = registry;
    }

    /**
     * Run method.
     *
     * @param args the program arguments
     */
    @Override
    public void run(ApplicationArguments args) {
        init(kafkaStreamsStarter);
    }

    /** {@inheritDoc} */
    @Override
    protected void startHttpServer() {
        // Nothing to do here as the server is already started by Spring Boot
    }

    /** {@inheritDoc} */
    @Override
    protected void initProperties() {
        serverPort = springBootServerPort;
        kafkaProperties = springBootKafkaProperties.asProperties();
        KafkaStreamsExecutionContext.registerProperties(kafkaProperties);
    }

    /** {@inheritDoc} */
    @Override
    protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse onStreamsUncaughtException(
            Throwable exception) {
        closeApplicationContext();
        return super.onStreamsUncaughtException(exception);
    }

    /** {@inheritDoc} */
    @Override
    protected void onStateChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState.equals(KafkaStreams.State.ERROR)) {
            closeApplicationContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void registerMetrics(KafkaStreams kafkaStreams) {
        // As the Kafka Streams metrics are not picked up by the OpenTelemetry Java agent automatically,
        // register them manually to the Spring Boot registry as the agent will pick metrics up from there
        KafkaStreamsMetrics kafkaStreamsMetrics = new KafkaStreamsMetrics(kafkaStreams);
        kafkaStreamsMetrics.bindTo(registry);
    }

    /** Close the application context. */
    private void closeApplicationContext() {
        if (applicationContext != null) {
            applicationContext.close();
        } else {
            log.warn("Spring Boot context is not set, cannot close it");
        }
    }
}
