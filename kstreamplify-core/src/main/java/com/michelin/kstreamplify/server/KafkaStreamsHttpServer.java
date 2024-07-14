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
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.service.KubernetesService;
import com.michelin.kstreamplify.service.TopologyService;
import com.michelin.kstreamplify.store.HostInfoResponse;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

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
                Object response = null;
                if (exchange.getRequestURI().toString().equals("/" + DEFAULT_STORE_PATH)) {
                    response = interactiveQueriesService.getStores();
                }

                if (exchange.getRequestURI().toString().matches("/" + DEFAULT_STORE_PATH + ".*/info")) {
                    String store = exchange.getRequestURI().toString().split("/")[2];
                    response = interactiveQueriesService.getStreamsMetadata(store)
                        .stream()
                        .map(streamsMetadata -> new HostInfoResponse(streamsMetadata.host(), streamsMetadata.port()))
                        .toList();
                }

                String jsonResponse = objectMapper.writeValueAsString(response);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, jsonResponse.length());
                exchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());

                OutputStream output = exchange.getResponseBody();
                output.write(jsonResponse.getBytes());

                exchange.close();
            }));
    }

    /**
     * Parse the query parameters.
     *
     * @param httpExchange The HTTP exchange
     * @return The query parameters
     */
    private Map<String, String> parseQueryParams(HttpExchange httpExchange) {
        List<String> parameters = Arrays.asList(httpExchange
            .getRequestURI()
            .toString()
            .split("\\?")[1]
            .split("&"));

        return parameters
            .stream()
            .map(param -> Pair.of(param.split("=")[0], param.split("=")[1]))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
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
