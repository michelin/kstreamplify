package com.michelin.kstreamplify.server;

import static com.michelin.kstreamplify.service.InteractiveQueriesService.DEFAULT_STORE_PATH;
import static com.michelin.kstreamplify.service.KubernetesService.DEFAULT_LIVENESS_PATH;
import static com.michelin.kstreamplify.service.KubernetesService.DEFAULT_READINESS_PATH;
import static com.michelin.kstreamplify.service.KubernetesService.LIVENESS_PATH_PROPERTY_NAME;
import static com.michelin.kstreamplify.service.KubernetesService.READINESS_PATH_PROPERTY_NAME;
import static com.michelin.kstreamplify.service.TopologyService.TOPOLOGY_DEFAULT_PATH;
import static com.michelin.kstreamplify.service.TopologyService.TOPOLOGY_PROPERTY;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.michelin.kstreamplify.exception.HttpServerException;
import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.service.KubernetesService;
import com.michelin.kstreamplify.service.TopologyService;
import com.michelin.kstreamplify.store.StreamsMetadata;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.function.IntSupplier;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;

/**
 * Kafka Streams HTTP server.
 */
public class KafkaStreamsHttpServer {
    private final KafkaStreamsInitializer kafkaStreamsInitializer;
    private final ObjectMapper objectMapper;
    private final KubernetesService kubernetesService;
    private final TopologyService topologyService;
    private final InteractiveQueriesService interactiveQueriesService;

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
        this.interactiveQueriesService = new InteractiveQueriesService(kafkaStreamsInitializer);
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
            .getOrDefault(TOPOLOGY_PROPERTY, TOPOLOGY_DEFAULT_PATH);

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
            return interactiveQueriesService.getStateStores();
        }

        String store;
        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH + "/metadata/.*")) {
            store = parsePathParam(exchange, 3);
            return interactiveQueriesService.getStreamsMetadataForStore(store)
                .stream()
                .map(streamsMetadata -> new StreamsMetadata(
                    streamsMetadata.stateStoreNames(),
                    streamsMetadata.hostInfo(),
                    streamsMetadata.topicPartitions()))
                .toList();
        }

        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH + "/local/.*")) {
            store = parsePathParam(exchange, 3);
            return interactiveQueriesService.getAllOnLocalhost(store);
        }

        store = parsePathParam(exchange, 2);
        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH + "/.*/.*")) {
            String key = parsePathParam(exchange, 3);
            return interactiveQueriesService.getByKey(store, key);
        }

        if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH + "/.*")) {
            return interactiveQueriesService.getAll(store);
        }

        return null;
    }

    private String parsePathParam(HttpExchange exchange, int index) {
        return exchange.getRequestURI()
            .toString()
            .split("\\?")[0]
            .split("/")[index];
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
