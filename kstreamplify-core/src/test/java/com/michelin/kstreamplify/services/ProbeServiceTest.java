package com.michelin.kstreamplify.services;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.model.RestServiceResponse;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.HttpURLConnection;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProbeServiceTest {

    @Mock
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private StreamThread streamThread;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
    }

    @Test
    void testReadinessProbeWithRunningStreams() {
        // Arrange
        KafkaStreamsExecutionContext.registerProperties(new Properties());
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        // Act
        RestServiceResponse<String> response = ProbeService.readinessProbe(kafkaStreamsInitializer);

        // Assert
        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    }

    @Test
    void testReadinessProbeWithNonRunningStreams() {
        // Arrange
        KafkaStreamsExecutionContext.registerProperties(new Properties());
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        // Act
        RestServiceResponse<String> response = ProbeService.readinessProbe(kafkaStreamsInitializer);

        // Assert
        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, response.getStatus());
    }

    @Test
    void testReadinessProbeWithNullKafkaStreams() {
        // Arrange
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        // Act
        RestServiceResponse<String> response = ProbeService.readinessProbe(kafkaStreamsInitializer);

        // Assert
        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getStatus());
    }

    @Test
    void testLivenessProbeWithRunningStreams() {
        // Arrange
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        // Act
        RestServiceResponse<String> response = ProbeService.livenessProbe(kafkaStreamsInitializer);

        // Assert
        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    }

    @Test
    void testLivenessProbeWithNonRunningStreams() {
        // Arrange
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        // Act
        RestServiceResponse<String> response = ProbeService.livenessProbe(kafkaStreamsInitializer);

        // Assert
        assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.getStatus());
    }

    @Test
    void testLivenessProbeWithNullKafkaStreams() {
        // Arrange
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        // Act
        RestServiceResponse<String> response = ProbeService.livenessProbe(kafkaStreamsInitializer);

        // Assert
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

