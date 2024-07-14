package com.michelin.kstreamplify.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.store.HostInfoResponse;
import com.michelin.kstreamplify.store.StateQueryData;
import com.michelin.kstreamplify.store.StateQueryResponse;
import java.util.List;
import org.apache.kafka.streams.StreamsMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InteractiveQueriesControllerTest {
    @Mock
    private StreamsMetadata streamsMetadata;

    @Mock
    private InteractiveQueriesService interactiveQueriesService;

    @InjectMocks
    private InteractiveQueriesController interactiveQueriesController;

    @Test
    void shouldGetStores() {
        when(interactiveQueriesService.getStores())
            .thenReturn(List.of("store1", "store2"));

        assertEquals(List.of("store1", "store2"), interactiveQueriesController.getStores().getBody());
    }

    @Test
    void shouldGetHostsForStore() {
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
    void shouldGetAll() {
        when(interactiveQueriesService.getAll("store", Object.class, Object.class))
            .thenReturn(List.of(new StateQueryData<>("key1", "value1", 1L,
                new HostInfoResponse("host1", 1234), List.of(new StateQueryResponse.PositionVector("topic1", 1, 1L)))));

        List<StateQueryResponse> responses = interactiveQueriesController.getAll("store", false, false).getBody();

        assertNotNull(responses);
        assertNull(responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertNull(responses.get(0).getTimestamp());
        assertNull(responses.get(0).getPositionVectors());
        assertNull(responses.get(0).getHostInfo());
    }

    @Test
    void shouldGetAllWithMetadata() {
        when(interactiveQueriesService.getAll("store", Object.class, Object.class))
            .thenReturn(List.of(new StateQueryData<>("key1", "value1", 1L,
                new HostInfoResponse("host1", 1234), List.of(new StateQueryResponse.PositionVector("topic1", 1, 1L)))));


        List<StateQueryResponse> responses = interactiveQueriesController.getAll("store", true, true).getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertEquals(1L, responses.get(0).getTimestamp());
        assertEquals("host1", responses.get(0).getHostInfo().host());
        assertEquals(1234, responses.get(0).getHostInfo().port());
        assertEquals("topic1", responses.get(0).getPositionVectors().get(0).topic());
        assertEquals(1, responses.get(0).getPositionVectors().get(0).partition());
        assertEquals(1L, responses.get(0).getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetByKey() {
        when(interactiveQueriesService.getByKey(eq("store"), eq("key"), any(), any()))
            .thenReturn(new StateQueryData<>("key1", "value1", 1L,
                new HostInfoResponse("host1", 1234), List.of(new StateQueryResponse.PositionVector("topic1", 1, 1L))));

        StateQueryResponse response = interactiveQueriesController.getByKey("store", "key", false, false).getBody();

        assertNotNull(response);
        assertNull(response.getKey());
        assertEquals("value1", response.getValue());
        assertNull(response.getTimestamp());
        assertNull(response.getPositionVectors());
        assertNull(response.getHostInfo());
    }

    @Test
    void shouldGetByKeyWithMetadata() {
        when(interactiveQueriesService.getByKey(eq("store"), eq("key"), any(), any()))
            .thenReturn(new StateQueryData<>("key1", "value1", 1L,
                new HostInfoResponse("host1", 1234), List.of(new StateQueryResponse.PositionVector("topic1", 1, 1L))));

        StateQueryResponse response = interactiveQueriesController.getByKey("store", "key", true, true).getBody();

        assertNotNull(response);
        assertEquals("key1", response.getKey());
        assertEquals("value1", response.getValue());
        assertEquals(1L, response.getTimestamp());
        assertEquals("host1", response.getHostInfo().host());
        assertEquals(1234, response.getHostInfo().port());
        assertEquals("topic1", response.getPositionVectors().get(0).topic());
        assertEquals(1, response.getPositionVectors().get(0).partition());
        assertEquals(1L, response.getPositionVectors().get(0).offset());
    }
}
