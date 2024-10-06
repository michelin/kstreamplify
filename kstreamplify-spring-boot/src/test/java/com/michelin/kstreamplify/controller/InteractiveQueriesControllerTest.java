package com.michelin.kstreamplify.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.service.interactivequeries.KeyValueStoreService;
import com.michelin.kstreamplify.service.interactivequeries.WindowStoreService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
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
    private KeyValueStoreService keyValueStoreService;

    @Mock
    private WindowStoreService windowStoreService;

    @InjectMocks
    private InteractiveQueriesController interactiveQueriesController;

    @Test
    void shouldGetStores() {
        when(keyValueStoreService.getStateStores())
            .thenReturn(Set.of("store1", "store2"));

        assertEquals(Set.of("store1", "store2"), interactiveQueriesController.getStores().getBody());
    }

    @Test
    void shouldGetStreamsMetadataForStore() {
        when(streamsMetadata.stateStoreNames())
            .thenReturn(Set.of("store"));

        when(streamsMetadata.hostInfo())
            .thenReturn(new HostInfo("host1", 1234));

        when(streamsMetadata.topicPartitions())
            .thenReturn(Set.of(new TopicPartition("topic", 0)));

        when(keyValueStoreService.getStreamsMetadataForStore("store"))
            .thenReturn(List.of(streamsMetadata));

        List<com.michelin.kstreamplify.store.StreamsMetadata> response =
            interactiveQueriesController.getStreamsMetadataForStore("store").getBody();

        assertNotNull(response);
        assertEquals(streamsMetadata.stateStoreNames(), response.get(0).getStateStoreNames());
        assertEquals(streamsMetadata.hostInfo().host(), response.get(0).getHostInfo().host());
        assertEquals(streamsMetadata.hostInfo().port(), response.get(0).getHostInfo().port());
        assertTrue(response.get(0).getTopicPartitions().contains("topic-0"));
    }

    @Test
    void shouldGetAllInKeyValueStore() {
        when(keyValueStoreService.getAll("store"))
            .thenReturn(List.of(new StateStoreRecord("key1", "value1", 1L)));

        List<StateStoreRecord> responses = interactiveQueriesController.getAllInKeyValueStore("store").getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertEquals(1L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetAllInKeyValueStoreOnLocalHost() {
        when(keyValueStoreService.getAllOnLocalHost("store"))
            .thenReturn(List.of(new StateStoreRecord("key1", "value1", 1L)));

        List<StateStoreRecord> responses = interactiveQueriesController.getAllInKeyValueStoreOnLocalHost("store")
            .getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertEquals(1L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetByKeyInKeyValueStore() {
        when(keyValueStoreService.getByKey("store", "key"))
            .thenReturn(new StateStoreRecord("key1", "value1", 1L));

        StateStoreRecord response = interactiveQueriesController
            .getByKeyInKeyValueStore("store", "key").getBody();

        assertNotNull(response);
        assertEquals("key1", response.getKey());
        assertEquals("value1", response.getValue());
        assertEquals(1L, response.getTimestamp());
    }

    @Test
    void shouldGetAllInWindowStore() {
        when(windowStoreService.getAll(any(), any(), any()))
            .thenReturn(List.of(new StateStoreRecord("key1", "value1", 1L)));

        List<StateStoreRecord> responses = interactiveQueriesController
            .getAllInWindowStore("store", Optional.of("1970-01-01T00:00:00Z"), Optional.of("1970-01-01T00:00:00Z"))
            .getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertEquals(1L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetAllInWindowStoreNoTimeFromNorTimeTo() {
        when(windowStoreService.getAll(any(), any(), any()))
            .thenReturn(List.of(new StateStoreRecord("key1", "value1", 1L)));

        List<StateStoreRecord> responses = interactiveQueriesController
            .getAllInWindowStore("store", Optional.empty(), Optional.empty())
            .getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertEquals(1L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetAllInWindowStoreOnLocalHost() {
        when(windowStoreService.getAllOnLocalHost(any(), any(), any()))
            .thenReturn(List.of(new StateStoreRecord("key1", "value1", 1L)));

        List<StateStoreRecord> responses = interactiveQueriesController
            .getAllInWindowStoreOnLocalHost(
                "store",
                Optional.of("1970-01-01T00:00:00Z"),
                Optional.of("1970-01-01T00:00:00Z"))
            .getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertEquals(1L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetAllInWindowStoreOnLocalHostNoTimeFromNorTimeTo() {
        when(windowStoreService.getAllOnLocalHost(any(), any(), any()))
            .thenReturn(List.of(new StateStoreRecord("key1", "value1", 1L)));

        List<StateStoreRecord> responses = interactiveQueriesController
            .getAllInWindowStoreOnLocalHost("store", Optional.empty(), Optional.empty())
            .getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertEquals(1L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetByKeyInWindowStore() {
        when(windowStoreService.getByKey(any(), any(), any(), any()))
            .thenReturn(List.of(new StateStoreRecord("key1", "value1", 1L)));

        List<StateStoreRecord> responses = interactiveQueriesController
            .getByKeyInWindowStore(
                "store",
                "key",
                Optional.of("1970-01-01T00:00:00Z"),
                Optional.of("1970-01-01T00:00:00Z"))
            .getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertEquals(1L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetByKeyInWindowStoreNoTimeFromNorTimeTo() {
        when(windowStoreService.getByKey(any(), any(), any(), any()))
            .thenReturn(List.of(new StateStoreRecord("key1", "value1", 1L)));

        List<StateStoreRecord> responses = interactiveQueriesController
            .getByKeyInWindowStore("store", "key", Optional.empty(), Optional.empty())
            .getBody();

        assertNotNull(responses);
        assertEquals("key1", responses.get(0).getKey());
        assertEquals("value1", responses.get(0).getValue());
        assertEquals(1L, responses.get(0).getTimestamp());
    }
}
