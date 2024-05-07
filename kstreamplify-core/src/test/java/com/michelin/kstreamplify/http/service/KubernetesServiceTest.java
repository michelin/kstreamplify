package com.michelin.kstreamplify.http.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import java.net.HttpURLConnection;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KubernetesServiceTest {
    @Mock
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    @Mock
    private KafkaStreams kafkaStreams;

    @InjectMocks
    private KubernetesService kubernetesService;

    @Test
    void shouldGetReadinessProbeWithWhenStreamsRunning() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        RestResponse<Void> response = kubernetesService.getReadiness();

        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    }

    @Test
    void shouldGetReadinessProbeWithWhenStreamsNotRunning() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        RestResponse<Void> response = kubernetesService.getReadiness();

        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, response.getStatus());
    }

    @Test
    void shouldGetReadinessProbeWithWhenStreamsNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        RestResponse<Void> response = kubernetesService.getReadiness();

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getStatus());
    }

    @Test
    void shouldGetLivenessProbeWithWhenStreamsRunning() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        RestResponse<Void> response = kubernetesService.getLiveness();

        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    }

    @Test
    void shouldGetLivenessProbeWithWhenStreamsNotRunning() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        RestResponse<Void> response = kubernetesService.getLiveness();

        assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.getStatus());
    }

    @Test
    void shouldGetLivenessProbeWithWhenStreamsNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        RestResponse<Void> response = kubernetesService.getLiveness();

        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.getStatus());
    }
}

