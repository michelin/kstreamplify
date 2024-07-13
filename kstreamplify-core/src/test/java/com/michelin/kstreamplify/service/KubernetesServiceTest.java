package com.michelin.kstreamplify.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.server.RestResponse;
import java.net.HttpURLConnection;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.ThreadMetadataImpl;
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
    void shouldGetReadinessProbeWhenRunning() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        RestResponse<Void> response = kubernetesService.getReadiness();

        assertEquals(HttpURLConnection.HTTP_OK, response.status());
    }

    @Test
    void shouldGetReadinessProbeWhenNotRunning() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        RestResponse<Void> response = kubernetesService.getReadiness();

        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, response.status());
    }

    @Test
    void shouldGetReadinessProbeWhenNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        RestResponse<Void> response = kubernetesService.getReadiness();

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.status());
    }

    @Test
    void shouldGetReadinessProbeWhenRebalancingAndAllThreadsCreated() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);
        when(kafkaStreams.metadataForLocalThreads()).thenReturn(Set.of(
            new ThreadMetadataImpl(
                "thread-1",
                StreamThread.State.CREATED.name(),
                "mainConsumerClientId",
                "restoreConsumerClientId",
                Set.of(),
                "adminClientId",
                Set.of(),
                Set.of())
        ));

        RestResponse<Void> response = kubernetesService.getReadiness();

        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.status());
    }

    @Test
    void shouldGetReadinessProbeWhenRebalancingAndAllThreadsNotStartingOrCreated() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);
        when(kafkaStreams.metadataForLocalThreads()).thenReturn(Set.of(
            new ThreadMetadataImpl(
                "thread-1",
                StreamThread.State.CREATED.name(),
                "mainConsumerClientId",
                "restoreConsumerClientId",
                Set.of(),
                "adminClientId",
                Set.of(),
                Set.of()),
            new ThreadMetadataImpl(
                "thread-2",
                StreamThread.State.STARTING.name(),
                "mainConsumerClientId",
                "restoreConsumerClientId",
                Set.of(),
                "adminClientId",
                Set.of(),
                Set.of()),
            new ThreadMetadataImpl(
                "thread-3",
                StreamThread.State.PARTITIONS_ASSIGNED.name(),
                "mainConsumerClientId",
                "restoreConsumerClientId",
                Set.of(),
                "adminClientId",
                Set.of(),
                Set.of())
        ));

        RestResponse<Void> response = kubernetesService.getReadiness();

        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, response.status());
    }

    @Test
    void shouldGetLivenessProbeWithWhenStreamsRunning() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        RestResponse<Void> response = kubernetesService.getLiveness();

        assertEquals(HttpURLConnection.HTTP_OK, response.status());
    }

    @Test
    void shouldGetLivenessProbeWithWhenStreamsNotRunning() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        RestResponse<Void> response = kubernetesService.getLiveness();

        assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.status());
    }

    @Test
    void shouldGetLivenessProbeWithWhenStreamsNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        RestResponse<Void> response = kubernetesService.getLiveness();

        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.status());
    }
}

