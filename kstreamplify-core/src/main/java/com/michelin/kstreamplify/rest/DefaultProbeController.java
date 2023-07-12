package com.michelin.kstreamplify.rest;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.sun.net.httpserver.HttpServer;
import com.michelin.kstreamplify.constants.DefaultProbeHttpServerConstants;
import com.michelin.kstreamplify.services.ProbeService;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
public class DefaultProbeController {
    protected HttpServer server;

    public DefaultProbeController(KafkaStreamsInitializer kafkaStreamsInitializer) {
        try {
            server = HttpServer.create(new InetSocketAddress(kafkaStreamsInitializer.getServerPort()), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var readinessPath = (String) kafkaStreamsInitializer.getProperties().get(DefaultProbeHttpServerConstants.READINESS_PROPERTY);
        var livenessPath = (String) kafkaStreamsInitializer.getProperties().get(DefaultProbeHttpServerConstants.LIVENESS_PROPERTY);
        var exposeTopologyPath = (String) kafkaStreamsInitializer.getProperties().get(DefaultProbeHttpServerConstants.TOPOLOGY_PROPERTY);

        readinessProbe(kafkaStreamsInitializer, '/' + (StringUtils.isBlank(readinessPath) ? DefaultProbeHttpServerConstants.READINESS_DEFAULT_PATH : readinessPath));
        livenessProbe(kafkaStreamsInitializer, '/' + (StringUtils.isBlank(livenessPath) ? DefaultProbeHttpServerConstants.LIVENESS_DEFAULT_PATH : livenessPath));
        exposeTopology(kafkaStreamsInitializer, '/' + (StringUtils.isBlank(exposeTopologyPath) ? DefaultProbeHttpServerConstants.TOPOLOGY_DEFAULT_PATH : exposeTopologyPath));
        customEndpointCaller(kafkaStreamsInitializer);
        server.start();
    }

    private void readinessProbe(KafkaStreamsInitializer kafkaStreamsInitializer, String readinessPath) {
        server.createContext(readinessPath, (exchange -> {
            exchange.sendResponseHeaders(ProbeService.readinessProbe(kafkaStreamsInitializer).getStatus(), 0);
            var output = exchange.getResponseBody();
            output.close();
            exchange.close();
        }));
    }

    /**
     * @param kafkaStreamsInitializer
     */

    private void livenessProbe(KafkaStreamsInitializer kafkaStreamsInitializer, String livenessPath) {
        server.createContext(livenessPath, (exchange -> {
            exchange.sendResponseHeaders(ProbeService.livenessProbe(kafkaStreamsInitializer).getStatus(), 0);
            var output = exchange.getResponseBody();
            output.close();
            exchange.close();
        }));
    }

    private void exposeTopology(KafkaStreamsInitializer kafkaStreamsInitializer, String exposeTopologyPath) {
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
     * Method to be overridden in case of custom endpoints. see other implems for details
     * @param kafkaStreamsInitializer
     */
    protected void customEndpointCaller(KafkaStreamsInitializer kafkaStreamsInitializer) {
    }
}
