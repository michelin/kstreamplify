package com.michelin.kstreamplify.http.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import java.net.HttpURLConnection;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for TopologyService.
 */
@ExtendWith(MockitoExtension.class)
public class TopologyServiceTest {
    @Mock
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    @InjectMocks
    private TopologyService topologyService;

    @Test
    void shouldExposeTopologyWithNonNullTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsStarter starter = new KafkaStreamsStarterImpl();
        starter.topology(streamsBuilder);

        when(kafkaStreamsInitializer.getTopology()).thenReturn(streamsBuilder.build());

        RestResponse<String> response = topologyService.getTopology();

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

        RestResponse<String> response = topologyService.getTopology();

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
