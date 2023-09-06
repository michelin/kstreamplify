package com.michelin.kstreamplify.rest;

import com.michelin.kstreamplify.constants.HttpServerConstants;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.services.ProbeService;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.commons.lang3.StringUtils;

/**
 * Default probe controller.
 */
public class DefaultProbeController {
    /**
     * HTTP server.
     */
    protected HttpServer server;

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    public DefaultProbeController(KafkaStreamsInitializer kafkaStreamsInitializer) {
        try {
            server =
                HttpServer.create(new InetSocketAddress(kafkaStreamsInitializer.getServerPort()),
                    0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var readinessPath = (String) kafkaStreamsInitializer.getProperties()
            .get(HttpServerConstants.READINESS_PROPERTY);
        var livenessPath = (String) kafkaStreamsInitializer.getProperties()
            .get(HttpServerConstants.LIVENESS_PROPERTY);
        var exposeTopologyPath = (String) kafkaStreamsInitializer.getProperties()
            .get(HttpServerConstants.TOPOLOGY_PROPERTY);

        readinessProbe(kafkaStreamsInitializer, '/'
            + (StringUtils.isBlank(readinessPath) ? HttpServerConstants.READINESS_DEFAULT_PATH :
                readinessPath));
        livenessProbe(kafkaStreamsInitializer, '/'
            + (StringUtils.isBlank(livenessPath) ? HttpServerConstants.LIVENESS_DEFAULT_PATH :
                livenessPath));
        exposeTopology(kafkaStreamsInitializer, '/'
            + (StringUtils.isBlank(exposeTopologyPath) ? HttpServerConstants.TOPOLOGY_DEFAULT_PATH :
                exposeTopologyPath));
        endpointCaller(kafkaStreamsInitializer);
        server.start();
    }

    /**
     * Kubernetes' readiness probe.
     */
    private void readinessProbe(KafkaStreamsInitializer kafkaStreamsInitializer,
                                String readinessPath) {
        server.createContext(readinessPath, (exchange -> {
            exchange.sendResponseHeaders(
                ProbeService.readinessProbe(kafkaStreamsInitializer).getStatus(), 0);
            var output = exchange.getResponseBody();
            output.close();
            exchange.close();
        }));
    }


    /**
     * Kubernetes' liveness probe.
     */
    private void livenessProbe(KafkaStreamsInitializer kafkaStreamsInitializer,
                               String livenessPath) {
        server.createContext(livenessPath, (exchange -> {
            exchange.sendResponseHeaders(
                ProbeService.livenessProbe(kafkaStreamsInitializer).getStatus(), 0);
            var output = exchange.getResponseBody();
            output.close();
            exchange.close();
        }));
    }

    /**
     * Get the Kafka Streams topology.
     */
    private void exposeTopology(KafkaStreamsInitializer kafkaStreamsInitializer,
                                String exposeTopologyPath) {
        server.createContext(exposeTopologyPath, (exchange -> {
            var restServiceResponse = ProbeService.exposeTopology(kafkaStreamsInitializer);

            exchange.sendResponseHeaders(restServiceResponse.getStatus(), 0);

            var output = exchange.getResponseBody();
            output.write((restServiceResponse.getBody()).getBytes());
            output.close();
            exchange.close();
        }));
    }


    /**
     * Callback to override in case of custom endpoint definition.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    protected void endpointCaller(KafkaStreamsInitializer kafkaStreamsInitializer) {
        // Nothing to do here
    }
}
