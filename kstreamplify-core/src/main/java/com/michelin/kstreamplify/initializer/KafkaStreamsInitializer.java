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

import static com.michelin.kstreamplify.property.PropertiesUtils.KAFKA_PROPERTIES_PREFIX;
import static java.util.Optional.ofNullable;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.property.PropertiesUtils;
import com.michelin.kstreamplify.server.KafkaStreamsHttpServer;
import java.util.HashMap;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;

/** The Kafka Streams initializer class. */
@Slf4j
@Getter
public class KafkaStreamsInitializer {
    /** The property holding the application server variable name. */
    public static final String APPLICATION_SERVER_PROPERTY_NAME = "application.server.var.name";

    /** The default application server variable name. */
    public static final String DEFAULT_APPLICATION_SERVER_VARIABLE_NAME = "APPLICATION_SERVER";

    /** The server port property name. */
    public static final String SERVER_PORT = "server.port";

    /** The Kafka Streams starter. */
    protected KafkaStreamsStarter kafkaStreamsStarter;

    /** The Kafka Streams instance. */
    protected KafkaStreams kafkaStreams;

    /** The Kafka Streams topology. */
    protected Topology topology;

    /** The whole properties loaded from the properties file. */
    protected Properties properties;

    /** The Kafka properties. */
    protected Properties kafkaProperties;

    /** The host information. */
    protected HostInfo hostInfo;

    /** The server port. */
    protected int serverPort;

    /**
     * Constructor.
     *
     * @param kafkaStreamsStarter The Kafka Streams starter
     */
    public KafkaStreamsInitializer(KafkaStreamsStarter kafkaStreamsStarter) {
        this.kafkaStreamsStarter = kafkaStreamsStarter;

        this.properties = PropertiesUtils.loadProperties();
        this.serverPort = (Integer) this.properties.get(SERVER_PORT);
        this.kafkaProperties = PropertiesUtils.extractSubProperties(properties, KAFKA_PROPERTIES_PREFIX, true);

        initExecutionContext();
        initHostInfo();
    }

    /**
     * Constructor. Used by specific initializers (e.g. Spring Boot) to directly pass the properties.
     *
     * @param kafkaStreamsStarter The Kafka Streams starter
     * @param serverPort The server port
     * @param kafkaProperties The Kafka properties
     */
    public KafkaStreamsInitializer(
            KafkaStreamsStarter kafkaStreamsStarter, int serverPort, Properties kafkaProperties) {
        this.kafkaStreamsStarter = kafkaStreamsStarter;

        this.serverPort = serverPort;
        this.kafkaProperties = kafkaProperties;

        initExecutionContext();
        initHostInfo();
    }

    /**
     * Start Kstreamplify.
     *
     * <ol>
     *   <li>Build the topology.
     *   <li>Create the Kafka Streams instance.
     *   <li>Start the Kafka Streams instance.
     *   <li>Start the HTTP server.
     * </ol>
     */
    public void start() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        kafkaStreamsStarter.topology(streamsBuilder);

        topology = streamsBuilder.build(KafkaStreamsExecutionContext.getProperties());
        log.info("Topology description:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, KafkaStreamsExecutionContext.getProperties());

        kafkaStreamsStarter.onStart(kafkaStreams);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.setUncaughtExceptionHandler(
                ofNullable(kafkaStreamsStarter.uncaughtExceptionHandler()).orElse(this::uncaughtExceptionHandler));

        kafkaStreams.setStateListener(this::stateListener);

        kafkaStreams.start();

        KafkaStreamsHttpServer server = new KafkaStreamsHttpServer(this);
        server.start();
    }

    /** Init the Kafka Streams execution context. */
    private void initExecutionContext() {
        KafkaStreamsExecutionContext.registerProperties(kafkaProperties);

        KafkaStreamsExecutionContext.setSerdesConfig(kafkaProperties.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> String.valueOf(e.getKey()),
                        e -> String.valueOf(e.getValue()),
                        (prev, next) -> next,
                        HashMap::new)));

        KafkaStreamsExecutionContext.setDlqTopicName(kafkaStreamsStarter.dlqTopic());
    }

    /** Init the host information. */
    private void initHostInfo() {
        String applicationServerVarName = (String) kafkaProperties.getOrDefault(
                APPLICATION_SERVER_PROPERTY_NAME, DEFAULT_APPLICATION_SERVER_VARIABLE_NAME);

        String applicationServer = System.getenv(applicationServerVarName);
        String host = StringUtils.isNotBlank(applicationServer) ? applicationServer : "localhost";

        hostInfo = new HostInfo(host, serverPort);

        log.info(
                "Kafka Streams \"{}\" is running on {}:{}",
                KafkaStreamsExecutionContext.getProperties().getProperty(APPLICATION_ID_CONFIG),
                hostInfo.host(),
                hostInfo.port());

        KafkaStreamsExecutionContext.getProperties()
                .put(APPLICATION_SERVER_CONFIG, "%s:%s".formatted(hostInfo.host(), hostInfo.port()));
    }

    /**
     * Default uncaught exception handler.
     *
     * @param exception The exception
     * @return The uncaught exception response
     */
    protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtExceptionHandler(
            Throwable exception) {
        log.error(
                "A not covered exception occurred in {} Kafka Streams. Shutting down...",
                kafkaProperties.get(APPLICATION_ID_CONFIG),
                exception);

        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }

    /**
     * Default state listener.
     *
     * @param newState The new state
     * @param oldState The old state
     */
    protected void stateListener(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState.equals(KafkaStreams.State.ERROR)) {
            log.error(
                    "The {} Kafka Streams is in error state. Shutting down...",
                    kafkaProperties.get(APPLICATION_ID_CONFIG));

            System.exit(3);
        }
    }

    /**
     * Check if the Kafka Streams is running.
     *
     * @return True if the Kafka Streams is running
     */
    public boolean isNotRunning() {
        return !kafkaStreams.state().equals(KafkaStreams.State.RUNNING);
    }
}
