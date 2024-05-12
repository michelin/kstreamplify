package com.michelin.kstreamplify.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.model.RestResponse;
import java.net.HttpURLConnection;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopologyServiceTest {
    @Mock
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    @InjectMocks
    private TopologyService topologyService;

    @Test
    void shouldExposeTopologyWithNonNullTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsStarter starter = new KafkaStreamsStarterStub();
        starter.topology(streamsBuilder);

        when(kafkaStreamsInitializer.getTopology()).thenReturn(streamsBuilder.build());

        RestResponse<String> response = topologyService.getTopology();

        assertEquals(HttpURLConnection.HTTP_OK, response.status());
        assertEquals("""
            Topologies:
               Sub-topology: 0
                Source: KSTREAM-SOURCE-0000000000 (topics: [INPUT_TOPIC])
                  --> KSTREAM-SINK-0000000001
                Sink: KSTREAM-SINK-0000000001 (topic: OUTPUT_TOPIC)
                  <-- KSTREAM-SOURCE-0000000000

            """, response.body());
    }

    @Test
    void shouldExposeTopologyWithNullTopology() {
        when(kafkaStreamsInitializer.getTopology()).thenReturn(null);

        RestResponse<String> response = topologyService.getTopology();

        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, response.status());
    }

    static class KafkaStreamsStarterStub extends KafkaStreamsStarter {
        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            streamsBuilder
                .stream("INPUT_TOPIC")
                .to("OUTPUT_TOPIC");
        }

        @Override
        public String dlqTopic() {
            return "DLQ_TOPIC";
        }
    }
}
