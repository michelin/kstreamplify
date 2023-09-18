package com.michelin.kstreamplify.services;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.model.RestServiceResponse;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.HttpURLConnection;
import java.util.Properties;
import java.util.Set;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProbeServiceTest {
    @Mock
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    @Mock
    private KafkaStreams kafkaStreams;

    @Test
    void shouldGetReadinessProbeWithRunningStreams() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        RestServiceResponse<String> response = ProbeService.readinessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    }

    @Test
    void testReadinessProbeWithNonRunningStreams() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        RestServiceResponse<String> response = ProbeService.readinessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, response.getStatus());
    }

    @Test
    void testReadinessProbeWithNullKafkaStreams() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        RestServiceResponse<String> response = ProbeService.readinessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getStatus());
    }

    @Test
    void testLivenessProbeWithRunningStreams() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        RestServiceResponse<String> response = ProbeService.livenessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    }

    @Test
    void testLivenessProbeWithNonRunningStreams() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        RestServiceResponse<String> response = ProbeService.livenessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.getStatus());
    }

    @Test
    void testLivenessProbeWithNullKafkaStreams() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        RestServiceResponse<String> response = ProbeService.livenessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.getStatus());
    }

    @Test
    void testExposeTopologyWithNonNullTopology() {
        // Arrange
        TopologyDescription topologyDescription =new TopologyDescription() {
            @Override
            public Set<Subtopology> subtopologies() {
                return null;
            }

            @Override
            public Set<GlobalStore> globalStores() {
                return null;
            }
        };
        when(kafkaStreamsInitializer.getTopology()).thenReturn(mock(org.apache.kafka.streams.Topology.class));
        when(kafkaStreamsInitializer.getTopology().describe()).thenReturn(topologyDescription);

        // Act
        RestServiceResponse<String> response = ProbeService.exposeTopology(kafkaStreamsInitializer);

        // Assert
        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    }

    @Test
    void testExposeTopologyWithNullTopology() {
        // Arrange
        when(kafkaStreamsInitializer.getTopology()).thenReturn(null);

        // Act
        RestServiceResponse<String> response = ProbeService.exposeTopology(kafkaStreamsInitializer);

        // Assert
        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.getStatus());
    }
}

