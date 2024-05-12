package com.michelin.kstreamplify.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.model.HostInfoResponse;
import com.michelin.kstreamplify.model.QueryResponse;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import java.util.List;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InteractiveQueriesControllerTest {
    private static final String STREAMS_NOT_STARTED = "Cannot process request while instance is in REBALANCING state";

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private StreamsMetadata streamsMetadata;

    @Mock
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    @Mock
    private InteractiveQueriesService interactiveQueriesService;

    @InjectMocks
    private InteractiveQueriesController interactiveQueriesController;

    @Test
    void shouldNotGetStoresWhenStreamsIsNotStarted() {
        when(interactiveQueriesService.getKafkaStreamsInitializer())
            .thenReturn(kafkaStreamsInitializer);

        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams())
            .thenReturn(kafkaStreams);

        when(kafkaStreams.state())
            .thenReturn(KafkaStreams.State.REBALANCING);

        StreamsNotStartedException exception = assertThrows(StreamsNotStartedException.class,
            () -> interactiveQueriesController.getStores());

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetStores() {
        when(interactiveQueriesService.getKafkaStreamsInitializer())
            .thenReturn(kafkaStreamsInitializer);

        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(false);

        when(interactiveQueriesService.getStores())
            .thenReturn(List.of("store1", "store2"));

        assertEquals(List.of("store1", "store2"), interactiveQueriesController.getStores().getBody());
    }

    @Test
    void shouldNotGetHostsForStoreWhenStreamsIsNotStarted() {
        when(interactiveQueriesService.getKafkaStreamsInitializer())
            .thenReturn(kafkaStreamsInitializer);

        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams())
            .thenReturn(kafkaStreams);

        when(kafkaStreams.state())
            .thenReturn(KafkaStreams.State.REBALANCING);

        StreamsNotStartedException exception = assertThrows(StreamsNotStartedException.class,
            () -> interactiveQueriesController.getHostsForStore("store"));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetHostsForStore() {
        when(interactiveQueriesService.getKafkaStreamsInitializer())
            .thenReturn(kafkaStreamsInitializer);

        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(false);

        when(interactiveQueriesService.getStreamsMetadata("store"))
            .thenReturn(List.of(streamsMetadata));

        when(streamsMetadata.host())
            .thenReturn("host1");

        when(streamsMetadata.port())
            .thenReturn(1234);

        assertEquals(List.of(new HostInfoResponse("host1", 1234)),
            interactiveQueriesController.getHostsForStore("store").getBody());
    }

    @Test
    void shouldNotGetAllWhenStreamsIsNotStarted() {
        when(interactiveQueriesService.getKafkaStreamsInitializer())
            .thenReturn(kafkaStreamsInitializer);

        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams())
            .thenReturn(kafkaStreams);

        when(kafkaStreams.state())
            .thenReturn(KafkaStreams.State.REBALANCING);

        StreamsNotStartedException exception = assertThrows(StreamsNotStartedException.class,
            () -> interactiveQueriesController.getAll("store", false, false));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetAll() {
        when(interactiveQueriesService.getKafkaStreamsInitializer())
            .thenReturn(kafkaStreamsInitializer);

        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(false);

        when(interactiveQueriesService.getAll("store", false, false))
            .thenReturn(List.of(new QueryResponse("key1", "value1")));

        List<QueryResponse> responses = interactiveQueriesController.getAll("store", false, false).getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
    }

    @Test
    void shouldNotGetByKeyWhenStreamsIsNotStarted() {
        when(interactiveQueriesService.getKafkaStreamsInitializer())
            .thenReturn(kafkaStreamsInitializer);

        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams())
            .thenReturn(kafkaStreams);

        when(kafkaStreams.state())
            .thenReturn(KafkaStreams.State.REBALANCING);

        StreamsNotStartedException exception = assertThrows(StreamsNotStartedException.class,
            () -> interactiveQueriesController.getByKey("store", "key", false, false));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetByKey() {
        when(interactiveQueriesService.getKafkaStreamsInitializer())
            .thenReturn(kafkaStreamsInitializer);

        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(false);

        when(interactiveQueriesService.getByKey(eq("store"), eq("key"), any(), eq(false), eq(false)))
            .thenReturn(new QueryResponse("key", "value"));

        QueryResponse response = interactiveQueriesController.getByKey("store", "key", false, false).getBody();

        assertNotNull(response);
        assertEquals("key", response.getKey());
        assertEquals("value", response.getValue());
    }
}
