package com.michelin.kstreamplify.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.exception.OtherInstanceResponseException;
import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.StateQueryData;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InteractiveQueriesServiceTest {
    private static final String STREAMS_NOT_STARTED = "Cannot process request while instance is in REBALANCING state";

    @Mock
    private KafkaStreamsInitializer kafkaStreamsInitializer;

    @Mock
    private HttpClient httpClient;

    @Mock
    private StreamsMetadata streamsMetadata;

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private StateQueryResult<KeyValueIterator<Object, ValueAndTimestamp<Object>>> stateRangeQueryResult;

    @Mock
    private KeyValueIterator<Object, ValueAndTimestamp<Object>> iterator;

    @Mock
    private StateQueryResult<ValueAndTimestamp<Object>> stateKeyQueryResult;

    @Mock
    private HttpResponse<String> httpResponse;

    @InjectMocks
    private InteractiveQueriesService interactiveQueriesService;

    @Test
    void shouldConstructInteractiveQueriesService() {
        InteractiveQueriesService service = new InteractiveQueriesService(kafkaStreamsInitializer);
        assertEquals(kafkaStreamsInitializer, service.getKafkaStreamsInitializer());
    }

    @Test
    void shouldNotGetStoresWhenStreamsIsNotStarted() {
        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams())
            .thenReturn(kafkaStreams);

        when(kafkaStreams.state())
            .thenReturn(KafkaStreams.State.REBALANCING);

        StreamsNotStartedException exception = assertThrows(StreamsNotStartedException.class,
            () -> interactiveQueriesService.getStores());

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetStores() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(List.of(streamsMetadata));
        when(streamsMetadata.stateStoreNames()).thenReturn(Set.of("store1", "store2"));

        List<String> stores = interactiveQueriesService.getStores();
        
        assertTrue(stores.contains("store1"));
        assertTrue(stores.contains("store2"));
    }

    @Test
    void shouldGetStoresNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(null);

        List<String> stores = interactiveQueriesService.getStores();

        assertTrue(stores.isEmpty());
    }

    @Test
    void shouldGetStoresEmpty() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(Collections.emptyList());

        List<String> stores = interactiveQueriesService.getStores();

        assertTrue(stores.isEmpty());
    }

    @Test
    void shouldNotGetStreamsMetadataForStoreWhenStreamsIsNotStarted() {
        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams())
            .thenReturn(kafkaStreams);

        when(kafkaStreams.state())
            .thenReturn(KafkaStreams.State.REBALANCING);

        StreamsNotStartedException exception = assertThrows(StreamsNotStartedException.class,
            () -> interactiveQueriesService.getStreamsMetadata("store"));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetStreamsMetadataForStore() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        Collection<StreamsMetadata> streamsMetadataResponse = interactiveQueriesService.getStreamsMetadata("store");

        assertIterableEquals(List.of(streamsMetadata), streamsMetadataResponse);
    }

    @Test
    void shouldNotGetAllWhenStreamsIsNotStarted() {
        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams())
            .thenReturn(kafkaStreams);

        when(kafkaStreams.state())
            .thenReturn(KafkaStreams.State.REBALANCING);

        StreamsNotStartedException exception = assertThrows(StreamsNotStartedException.class,
            () -> interactiveQueriesService.getAll("store", Object.class, Object.class));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetAllThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(null);

        assertThrows(UnknownStateStoreException.class, () -> interactiveQueriesService.getAll("store",
            Object.class, Object.class));
    }

    @Test
    void shouldGetAllThrowsUnknownStoreExceptionWhenMetadataEmpty() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(Collections.emptyList());

        assertThrows(UnknownStateStoreException.class, () -> interactiveQueriesService.getAll("store",
            Object.class, Object.class));
    }

    @Test
    void shouldGetAllCurrentInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(streamsMetadata.hostInfo()).thenReturn(hostInfo);
        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(hostInfo);

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<KeyValueIterator<Object,
            ValueAndTimestamp<Object>>>>any())).thenReturn(stateRangeQueryResult);

        QueryResult<KeyValueIterator<Object, ValueAndTimestamp<Object>>> queryResult = QueryResult.forResult(iterator);
        queryResult.setPosition(Position.fromMap(Map.of("topic", Map.of(0, 15L))));
        when(stateRangeQueryResult.getPartitionResults()).thenReturn(Map.of(0, queryResult));
        doCallRealMethod().when(iterator).forEachRemaining(any());
        when(iterator.hasNext())
            .thenReturn(true)
            .thenReturn(false);

        PersonStub personStub = new PersonStub("John", "Doe");
        when(iterator.next())
            .thenReturn(KeyValue.pair("key", ValueAndTimestamp.make(personStub, 150L)));

        List<StateQueryData<String, PersonStub>> responses = interactiveQueriesService.getAll("store",
            String.class, PersonStub.class);

        assertEquals("key", responses.get(0).getKey());
        assertEquals(personStub, responses.get(0).getValue());
        assertEquals(150L, responses.get(0).getTimestamp());
        assertEquals("localhost", responses.get(0).getHostInfo().host());
        assertEquals(8080, responses.get(0).getHostInfo().port());
        assertEquals("topic", responses.get(0).getPositionVectors().get(0).topic());
        assertEquals(0, responses.get(0).getPositionVectors().get(0).partition());
        assertEquals(15L, responses.get(0).getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetAllOtherInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(streamsMetadata.hostInfo()).thenReturn(hostInfo);

        HostInfo anotherHostInfo = new HostInfo("anotherHost", 8080);
        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(anotherHostInfo);

        when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofString())))
            .thenReturn(CompletableFuture.completedFuture(httpResponse));
        when(httpResponse.body()).thenReturn("""
            [
              {
                "key": "key",
                "value": {
                  "firstName": "John",
                  "lastName": "Doe"
                },
                "timestamp": 150,
                "hostInfo": {
                  "host": "localhost",
                  "port": 8080
                },
                "positionVectors": [
                  {
                    "topic": "topic",
                    "partition": 0,
                    "offset": 15
                  }
                ]
              }
            ]""");

        List<StateQueryData<String, PersonStub>> responses = interactiveQueriesService.getAll("store",
            String.class, PersonStub.class);

        assertEquals("key", responses.get(0).getKey());
        assertEquals(new PersonStub("John", "Doe"), responses.get(0).getValue());
        assertEquals(150L, responses.get(0).getTimestamp());
        assertEquals("localhost", responses.get(0).getHostInfo().host());
        assertEquals(8080, responses.get(0).getHostInfo().port());
        assertEquals("topic", responses.get(0).getPositionVectors().get(0).topic());
        assertEquals(0, responses.get(0).getPositionVectors().get(0).partition());
        assertEquals(15L, responses.get(0).getPositionVectors().get(0).offset());
    }

    @Test
    void shouldHandleRuntimeExceptionWhenGettingAllOtherInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(streamsMetadata.hostInfo()).thenReturn(hostInfo);

        HostInfo anotherHostInfo = new HostInfo("anotherHost", 8080);
        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(anotherHostInfo);

        when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofString())))
            .thenThrow(new RuntimeException("Error"));

        OtherInstanceResponseException exception = assertThrows(OtherInstanceResponseException.class,
            () -> interactiveQueriesService.getAll("store", String.class, PersonStub.class));

        assertEquals("Fail to read other instance response", exception.getMessage());
    }

    @Test
    void shouldNotGetByKeyWhenStreamsIsNotStarted() {
        when(kafkaStreamsInitializer.isNotRunning())
            .thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams())
            .thenReturn(kafkaStreams);

        when(kafkaStreams.state())
            .thenReturn(KafkaStreams.State.REBALANCING);

        try (StringSerializer stringSerializer = new StringSerializer()) {
            StreamsNotStartedException exception = assertThrows(StreamsNotStartedException.class,
                () -> interactiveQueriesService.getByKey("store", "key", stringSerializer, Object.class));

            assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
        }
    }

    @Test
    void shouldGetByKeyThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(null);

        try (StringSerializer stringSerializer = new StringSerializer()) {
            assertThrows(UnknownStateStoreException.class, () ->
                interactiveQueriesService.getByKey("store", "key", stringSerializer, Object.class));
        }
    }

    @Test
    void shouldGetByKeyCurrentInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(new KeyQueryMetadata(new HostInfo("localhost", 8080), Collections.emptySet(), 0));

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(hostInfo);

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<ValueAndTimestamp<Object>>>any()))
            .thenReturn(stateKeyQueryResult);

        QueryResult<ValueAndTimestamp<Object>> queryResult = QueryResult
            .forResult(ValueAndTimestamp.make(new PersonStub("John", "Doe"), 150L));
        queryResult.setPosition(Position.fromMap(Map.of("topic", Map.of(0, 15L))));

        when(stateKeyQueryResult.getOnlyPartitionResult())
            .thenReturn(queryResult);

        StateQueryData<String, PersonStub> response = interactiveQueriesService
            .getByKey("store", "key", new StringSerializer(), PersonStub.class);

        assertEquals("key", response.getKey());
        assertEquals(new PersonStub("John", "Doe"), response.getValue());
        assertEquals(150L, response.getTimestamp());
        assertEquals("localhost", response.getHostInfo().host());
        assertEquals(8080, response.getHostInfo().port());
        assertEquals("topic", response.getPositionVectors().get(0).topic());
        assertEquals(0, response.getPositionVectors().get(0).partition());
        assertEquals(15L, response.getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetByKeyOtherInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(new KeyQueryMetadata(new HostInfo("localhost", 8085), Collections.emptySet(), 0));

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(hostInfo);

        when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofString())))
            .thenReturn(CompletableFuture.completedFuture(httpResponse));
        when(httpResponse.body()).thenReturn("""              
              {
                "key": "key",
                "value": {
                  "firstName": "John",
                  "lastName": "Doe"
                },
                "timestamp": 150,
                "hostInfo": {
                  "host": "localhost",
                  "port": 8080
                },
                "positionVectors": [
                  {
                    "topic": "topic",
                    "partition": 0,
                    "offset": 15
                  }
                ]
              }
            """);

        StateQueryData<String, PersonStub> response = interactiveQueriesService
            .getByKey("store", "key", new StringSerializer(), PersonStub.class);

        assertEquals("key", response.getKey());
        assertEquals(new PersonStub("John", "Doe"), response.getValue());
        assertEquals(150L, response.getTimestamp());
        assertEquals("localhost", response.getHostInfo().host());
        assertEquals(8080, response.getHostInfo().port());
        assertEquals("topic", response.getPositionVectors().get(0).topic());
        assertEquals(0, response.getPositionVectors().get(0).partition());
        assertEquals(15L, response.getPositionVectors().get(0).offset());
    }

    @Test
    void shouldGetUnknownKeyCurrentInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(new KeyQueryMetadata(new HostInfo("localhost", 8080), Collections.emptySet(), 0));

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(hostInfo);

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<ValueAndTimestamp<Object>>>any()))
            .thenReturn(stateKeyQueryResult);

        QueryResult<ValueAndTimestamp<Object>> queryResult = QueryResult
            .forResult(ValueAndTimestamp.make(new PersonStub("John", "Doe"), 150L));
        queryResult.setPosition(Position.fromMap(Map.of("topic", Map.of(0, 15L))));

        when(stateKeyQueryResult.getOnlyPartitionResult())
            .thenReturn(null);

        try (StringSerializer stringSerializer = new StringSerializer()) {
            UnknownKeyException exception = assertThrows(UnknownKeyException.class, () ->
                interactiveQueriesService.getByKey("store", "unknownKey", stringSerializer, PersonStub.class));

            assertEquals("Key unknownKey not found", exception.getMessage());
        }
    }

    @Test
    void shouldHandleRuntimeExceptionWhenGettingByKeyOtherInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(new KeyQueryMetadata(new HostInfo("localhost", 8085), Collections.emptySet(), 0));

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(hostInfo);

        when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofString())))
            .thenThrow(new RuntimeException("Error"));

        try (StringSerializer stringSerializer = new StringSerializer()) {
            OtherInstanceResponseException exception = assertThrows(OtherInstanceResponseException.class,
                () -> interactiveQueriesService.getByKey("store", "key", stringSerializer, PersonStub.class));

            assertEquals("Fail to read other instance response", exception.getMessage());
        }
    }

    record PersonStub(String firstName, String lastName) { }
}
