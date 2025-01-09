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

package com.michelin.kstreamplify.server;

import static com.michelin.kstreamplify.service.KubernetesService.DEFAULT_LIVENESS_PATH;
import static com.michelin.kstreamplify.service.KubernetesService.DEFAULT_READINESS_PATH;
import static com.michelin.kstreamplify.service.KubernetesService.LIVENESS_PATH_PROPERTY_NAME;
import static com.michelin.kstreamplify.service.KubernetesService.READINESS_PATH_PROPERTY_NAME;
import static com.michelin.kstreamplify.service.TopologyService.TOPOLOGY_DEFAULT_PATH;
import static com.michelin.kstreamplify.service.TopologyService.TOPOLOGY_PATH_PROPERTY_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.michelin.kstreamplify.exception.HttpServerException;
import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.service.KubernetesService;
import com.michelin.kstreamplify.service.TopologyService;
import com.michelin.kstreamplify.service.interactivequeries.KeyValueStoreService;
import com.michelin.kstreamplify.service.interactivequeries.WindowStoreService;
import com.michelin.kstreamplify.store.StreamsMetadata;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;

/**
 * Kafka Streams HTTP server.
 */
public class KafkaStreamsHttpServer {
    private static final String DEFAULT_STORE_PATH = "store";
    private static final String DEFAULT_KEY_VALUE_STORE_PATH = "key-value";
    private static final String DEFAULT_WINDOW_STORE_PATH = "window";
    private static final String TIME_FROM_REQUEST_PARAM = "timeFrom";
    private static final String TIME_TO_REQUEST_PARAM = "timeTo";
    private final KafkaStreamsInitializer kafkaStreamsInitializer;
    private final ObjectMapper objectMapper;
    private final KubernetesService kubernetesService;
    private final TopologyService topologyService;
    private final KeyValueStoreService keyValueStoreService;
    private final WindowStoreService windowStoreService;

    /**
     * The HTTP server.
     */
    protected HttpServer server;

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    public KafkaStreamsHttpServer(KafkaStreamsInitializer kafkaStreamsInitializer) {
        this.kafkaStreamsInitializer = kafkaStreamsInitializer;
        this.objectMapper = new ObjectMapper();
        this.kubernetesService = new KubernetesService(kafkaStreamsInitializer);
        this.topologyService = new TopologyService(kafkaStreamsInitializer);
        this.keyValueStoreService = new KeyValueStoreService(kafkaStreamsInitializer);
        this.windowStoreService = new WindowStoreService(kafkaStreamsInitializer);
    }

    /**
     * Start the HTTP server.
     */
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(kafkaStreamsInitializer.getServerPort()), 0);

            Properties properties = kafkaStreamsInitializer.getProperties();

            createKubernetesEndpoint(
                (String) properties.getOrDefault(READINESS_PATH_PROPERTY_NAME, DEFAULT_READINESS_PATH),
                kubernetesService::getReadiness);

            createKubernetesEndpoint(
                (String) properties.getOrDefault(LIVENESS_PATH_PROPERTY_NAME, DEFAULT_LIVENESS_PATH),
                kubernetesService::getLiveness);

            createTopologyEndpoint();
            createStoreEndpoints();

            addEndpoint(kafkaStreamsInitializer);
            server.start();
        } catch (Exception e) {
            throw new HttpServerException(e);
        }
    }

    private void createKubernetesEndpoint(String path, IntSupplier kubernetesSupplier) {
        server.createContext("/" + path,
            (exchange -> {
                int code = kubernetesSupplier.getAsInt();
                exchange.sendResponseHeaders(code, 0);
                exchange.close();
            }));
    }

    private void createTopologyEndpoint() {
        String topologyEndpointPath = (String) kafkaStreamsInitializer.getProperties()
            .getOrDefault(TOPOLOGY_PATH_PROPERTY_NAME, TOPOLOGY_DEFAULT_PATH);

        server.createContext("/" + topologyEndpointPath,
            (exchange -> {
                String response = topologyService.getTopology();
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length());
                exchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, MediaType.PLAIN_TEXT_UTF_8.toString());

                OutputStream output = exchange.getResponseBody();
                output.write(response.getBytes());

                exchange.close();
            }));
    }

    private void createStoreEndpoints() {
        server.createContext("/" + DEFAULT_STORE_PATH,
            (exchange -> {
                try {
                    Object response = getResponseForStoreEndpoints(exchange);
                    String jsonResponse = objectMapper.writeValueAsString(response);

                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, jsonResponse.length());
                    exchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());

                    OutputStream output = exchange.getResponseBody();
                    output.write(jsonResponse.getBytes());
                } catch (StreamsNotStartedException e) {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, e.getMessage().length());
                    OutputStream output = exchange.getResponseBody();
                    output.write(e.getMessage().getBytes());
                } catch (UnknownStateStoreException | UnknownKeyException e) {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, e.getMessage().length());
                    OutputStream output = exchange.getResponseBody();
                    output.write(e.getMessage().getBytes());
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage().length());
                    OutputStream output = exchange.getResponseBody();
                    output.write(e.getMessage().getBytes());
                } finally {
                    exchange.close();
                }
            }));
    }

    private Object getResponseForStoreEndpoints(HttpExchange exchange) {
        if (exchange.getRequestURI().toString().equals("/" + DEFAULT_STORE_PATH)) {
            return keyValueStoreService.getStateStores();
        }

        String store;
        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH + "/metadata/.*")) {
            store = parsePathParam(exchange, 3);
            return keyValueStoreService.getStreamsMetadataForStore(store)
                .stream()
                .map(streamsMetadata -> new StreamsMetadata(
                    streamsMetadata.stateStoreNames(),
                    streamsMetadata.hostInfo(),
                    streamsMetadata.topicPartitions()))
                .toList();
        }

        if (exchange.getRequestURI().toString()
            .matches("/" + DEFAULT_STORE_PATH + "/" + DEFAULT_KEY_VALUE_STORE_PATH + "/local/.*")) {
            store = parsePathParam(exchange, 4);
            return keyValueStoreService.getAllOnLocalHost(store);
        }

        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH
            + "/" + DEFAULT_WINDOW_STORE_PATH + "/local/.*")) {
            store = parsePathParam(exchange, 4);
            Instant instantFrom = parseRequestParam(exchange, TIME_FROM_REQUEST_PARAM)
                .map(Instant::parse)
                .orElse(Instant.EPOCH);

            Instant instantTo = parseRequestParam(exchange, TIME_TO_REQUEST_PARAM)
                .map(Instant::parse)
                .orElse(Instant.now());
            return windowStoreService.getAllOnLocalHost(store, instantFrom, instantTo);
        }

        store = parsePathParam(exchange, 3);
        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH
            + "/" + DEFAULT_KEY_VALUE_STORE_PATH + "/.*/.*")) {
            String key = parsePathParam(exchange, 4);
            return keyValueStoreService.getByKey(store, key);
        }

        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH
            + "/" + DEFAULT_KEY_VALUE_STORE_PATH + "/.*")) {
            return keyValueStoreService.getAll(store);
        }

        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH
            + "/" + DEFAULT_WINDOW_STORE_PATH + "/.*/.*")) {
            String key = parsePathParam(exchange, 4);
            Instant instantFrom = parseRequestParam(exchange, TIME_FROM_REQUEST_PARAM)
                .map(Instant::parse)
                .orElse(Instant.EPOCH);

            Instant instantTo = parseRequestParam(exchange, TIME_TO_REQUEST_PARAM)
                .map(Instant::parse)
                .orElse(Instant.now());
            return windowStoreService.getByKey(store, key, instantFrom, instantTo);
        }

        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH
            + "/" + DEFAULT_WINDOW_STORE_PATH + "/.*")) {
            Instant instantFrom = parseRequestParam(exchange, TIME_FROM_REQUEST_PARAM)
                .map(Instant::parse)
                .orElse(Instant.EPOCH);

            Instant instantTo = parseRequestParam(exchange, TIME_TO_REQUEST_PARAM)
                .map(Instant::parse)
                .orElse(Instant.now());
            return windowStoreService.getAll(store, instantFrom, instantTo);
        }

        return null;
    }

    private String parsePathParam(HttpExchange exchange, int index) {
        return exchange.getRequestURI()
            .toString()
            .split("\\?")[0]
            .split("/")[index];
    }

    private Optional<String> parseRequestParam(HttpExchange exchange, String key) {
        String[] uriAndParams = exchange.getRequestURI()
            .toString()
            .split("\\?");

        if (uriAndParams.length == 1) {
            return Optional.empty();
        }

        List<String> params = Arrays.asList(uriAndParams[1]
            .split("&"));

        Map<String, String> keyValue = params
            .stream()
            .map(param -> param.split("="))
            .collect(Collectors.toMap(param -> param[0], param -> param[1]));

        return Optional.ofNullable(keyValue.get(key));
    }

    /**
     * Callback to override in case of adding endpoints.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    protected void addEndpoint(KafkaStreamsInitializer kafkaStreamsInitializer) {
        // Nothing to do here
    }
}
