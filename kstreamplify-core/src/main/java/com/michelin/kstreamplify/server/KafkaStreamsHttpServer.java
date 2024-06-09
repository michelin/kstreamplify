package com.michelin.kstreamplify.server;

import static com.michelin.kstreamplify.service.KubernetesService.DEFAULT_LIVENESS_PATH;
import static com.michelin.kstreamplify.service.KubernetesService.DEFAULT_READINESS_PATH;
import static com.michelin.kstreamplify.service.KubernetesService.LIVENESS_PATH_PROPERTY_NAME;
import static com.michelin.kstreamplify.service.KubernetesService.READINESS_PATH_PROPERTY_NAME;
import static com.michelin.kstreamplify.service.TopologyService.TOPOLOGY_DEFAULT_PATH;
import static com.michelin.kstreamplify.service.TopologyService.TOPOLOGY_PROPERTY;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.service.KubernetesService;
import com.michelin.kstreamplify.service.TopologyService;
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
    private final KubernetesService kubernetesService;
    private final TopologyService topologyService;

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
        this.kubernetesService = new KubernetesService(kafkaStreamsInitializer);
        this.topologyService = new TopologyService(kafkaStreamsInitializer);
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
            .getOrDefault(READINESS_PATH_PROPERTY_NAME, DEFAULT_READINESS_PATH), kubernetesService::getReadiness));
        httpEndpoints.add(new HttpEndpoint((String) kafkaStreamsInitializer.getProperties()
            .getOrDefault(LIVENESS_PATH_PROPERTY_NAME, DEFAULT_LIVENESS_PATH), kubernetesService::getLiveness));
        httpEndpoints.add(new HttpEndpoint((String) kafkaStreamsInitializer.getProperties()
            .getOrDefault(TOPOLOGY_PROPERTY, TOPOLOGY_DEFAULT_PATH), topologyService::getTopology));

        httpEndpoints.forEach(this::exposeEndpoint);

        addEndpoint(kafkaStreamsInitializer);
        server.start();
    }

    /**
     * Expose an endpoint.
     *
     * @param httpEndpoint The endpoint
     */
    private void exposeEndpoint(HttpEndpoint httpEndpoint) {
        server.createContext("/" + httpEndpoint.getPath(), (exchange -> {
            RestResponse<?> restResponse = httpEndpoint.getRestService().get();
            exchange.sendResponseHeaders(restResponse.status(), 0);

            OutputStream output = exchange.getResponseBody();
            if (restResponse.body() != null) {
                output.write(((String) restResponse.body()).getBytes());
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
