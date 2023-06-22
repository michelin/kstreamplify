package io.github.michelin.spring.kafka.streams.rest;

import com.sun.net.httpserver.HttpServer;
import io.github.michelin.spring.kafka.streams.initializer.KafkaStreamsInitializer;
import io.github.michelin.spring.kafka.streams.services.ProbeService;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.github.michelin.spring.kafka.streams.constants.DefaultProbeHttpServerConstants.*;

public class DefaultProbeController {
    protected HttpServer server;

    public DefaultProbeController(KafkaStreamsInitializer kafkaStreamsInitializer) {
        try {
            server = HttpServer.create(new InetSocketAddress(kafkaStreamsInitializer.getServerPort()), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        var readinessPath = (String) kafkaStreamsInitializer.getProperties().get(READINESS_PROPERTY);
        var livenessPath = (String) kafkaStreamsInitializer.getProperties().get(LIVENESS_PROPERTY);
        var exposeTopologyPath = (String) kafkaStreamsInitializer.getProperties().get(TOPOLOGY_PROPERTY);

        readinessProbe(kafkaStreamsInitializer, '/' + (StringUtils.isBlank(readinessPath) ? READINESS_DEFAULT_PATH : readinessPath));
        livenessProbe(kafkaStreamsInitializer, '/' + (StringUtils.isBlank(livenessPath) ? LIVENESS_DEFAULT_PATH : livenessPath));
        exposeTopology(kafkaStreamsInitializer, '/' + (StringUtils.isBlank(exposeTopologyPath) ? TOPOLOGY_DEFAULT_PATH : exposeTopologyPath));
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
