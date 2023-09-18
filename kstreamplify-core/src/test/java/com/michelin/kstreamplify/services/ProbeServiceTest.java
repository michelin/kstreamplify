package com.michelin.kstreamplify.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.model.RestServiceResponse;
import java.net.HttpURLConnection;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProbeServiceTest {
    @Mock
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    @Mock
    private KafkaStreams kafkaStreams;

    @Test
    void shouldGetReadinessProbeWithWhenStreamsRunning() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        RestServiceResponse<String> response = ProbeService.readinessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    }

    @Test
    void shouldGetReadinessProbeWithWhenStreamsNotRunning() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        RestServiceResponse<String> response = ProbeService.readinessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, response.getStatus());
    }

    @Test
    void shouldGetReadinessProbeWithWhenStreamsNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        RestServiceResponse<String> response = ProbeService.readinessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getStatus());
    }

    @Test
    void shouldGetLivenessProbeWithWhenStreamsRunning() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        RestServiceResponse<String> response = ProbeService.livenessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
    }

    @Test
    void shouldGetLivenessProbeWithWhenStreamsNotRunning() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);

        RestServiceResponse<String> response = ProbeService.livenessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.getStatus());
    }

    @Test
    void shouldGetLivenessProbeWithWhenStreamsNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(null);

        RestServiceResponse<String> response = ProbeService.livenessProbe(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.getStatus());
    }

    @Test
    void shouldExposeTopologyWithNonNullTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsStarter starter = new KafkaStreamsStarterImpl();
        starter.topology(streamsBuilder);

        when(kafkaStreamsInitializer.getTopology()).thenReturn(streamsBuilder.build());

        RestServiceResponse<String> response = ProbeService.exposeTopology(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_OK, response.getStatus());
        assertEquals("""
            Topologies:
               Sub-topology: 0
                Source: KSTREAM-SOURCE-0000000000 (topics: [inputTopic])
                  --> KSTREAM-SINK-0000000001
                Sink: KSTREAM-SINK-0000000001 (topic: outputTopic)
                  <-- KSTREAM-SOURCE-0000000000

            """, response.getBody());
    }

    @Test
    void shouldExposeTopologyWithNullTopology() {
        when(kafkaStreamsInitializer.getTopology()).thenReturn(null);

        RestServiceResponse<String> response = ProbeService.exposeTopology(kafkaStreamsInitializer);

        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.getStatus());
    }

    static class KafkaStreamsStarterImpl extends KafkaStreamsStarter {
        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder
                .stream("inputTopic")
                .to("outputTopic");
        }

        @Override
        public String dlqTopic() {
            return "dlqTopic";
        }
    }
}

