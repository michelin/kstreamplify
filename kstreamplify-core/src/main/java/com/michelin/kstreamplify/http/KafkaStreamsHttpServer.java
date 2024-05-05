package com.michelin.kstreamplify.http;

import static com.michelin.kstreamplify.kubernetes.KubernetesService.DEFAULT_LIVENESS_PATH;
import static com.michelin.kstreamplify.kubernetes.KubernetesService.DEFAULT_READINESS_PATH;
import static com.michelin.kstreamplify.kubernetes.KubernetesService.LIVENESS_PATH_PROPERTY_NAME;
import static com.michelin.kstreamplify.kubernetes.KubernetesService.READINESS_PATH_PROPERTY_NAME;
import static com.michelin.kstreamplify.topology.TopologyService.TOPOLOGY_DEFAULT_PATH;
import static com.michelin.kstreamplify.topology.TopologyService.TOPOLOGY_PROPERTY;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.kubernetes.KubernetesService;
import com.michelin.kstreamplify.model.RestServiceResponse;
import com.michelin.kstreamplify.topology.TopologyService;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Kafka Streams HTTP server.
 */
public class KafkaStreamsHttpServer {
    private final KafkaStreamsInitializer kafkaStreamsInitializer;

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
    }

    /**
     * Start the HTTP server.
     */
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(kafkaStreamsInitializer.getServerPort()), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<HttpEndpoint> httpEndpoints = new ArrayList<>();
        httpEndpoints.add(new HttpEndpoint((String) kafkaStreamsInitializer.getProperties()
            .getOrDefault(READINESS_PATH_PROPERTY_NAME, DEFAULT_READINESS_PATH), KubernetesService::getReadiness));
        httpEndpoints.add(new HttpEndpoint((String) kafkaStreamsInitializer.getProperties()
            .getOrDefault(LIVENESS_PATH_PROPERTY_NAME, DEFAULT_LIVENESS_PATH), KubernetesService::getLiveness));
        httpEndpoints.add(new HttpEndpoint((String) kafkaStreamsInitializer.getProperties()
            .getOrDefault(TOPOLOGY_PROPERTY, TOPOLOGY_DEFAULT_PATH), TopologyService::getTopology));

        httpEndpoints.forEach(httpEndpoint -> exposeEndpoint(kafkaStreamsInitializer, httpEndpoint));

        addEndpoint(kafkaStreamsInitializer);
        server.start();
    }

    /**
     * Expose an endpoint.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @param httpEndpoint            The endpoint
     */
    private void exposeEndpoint(KafkaStreamsInitializer kafkaStreamsInitializer, HttpEndpoint httpEndpoint) {
        server.createContext("/" + httpEndpoint.getPath(), (exchange -> {
            RestServiceResponse<?> restServiceResponse = httpEndpoint.getRestService()
                .apply(kafkaStreamsInitializer);
            exchange.sendResponseHeaders(restServiceResponse.getStatus(), 0);

            OutputStream output = exchange.getResponseBody();
            if (restServiceResponse.getBody() != null) {
                output.write(((String) restServiceResponse.getBody()).getBytes());
            }
            output.close();
            exchange.close();
        }));
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
