package com.michelin.kstreamplify.service.interactivequeries;

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
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
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
class KeyValueStoreServiceTest {
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
    private KeyValueStoreService keyValueStoreService;

    @Test
    void shouldConstructInteractiveQueriesService() {
        KeyValueStoreService service = new KeyValueStoreService(kafkaStreamsInitializer);
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
            () -> keyValueStoreService.getStateStores());

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetStores() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(List.of(streamsMetadata));
        when(streamsMetadata.stateStoreNames()).thenReturn(Set.of("store1", "store2"));

        Set<String> stores = keyValueStoreService.getStateStores();
        
        assertTrue(stores.contains("store1"));
        assertTrue(stores.contains("store2"));
    }

    @Test
    void shouldGetStoresWhenNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(null);

        Set<String> stores = keyValueStoreService.getStateStores();

        assertTrue(stores.isEmpty());
    }

    @Test
    void shouldGetStoresWhenEmpty() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(Collections.emptyList());

        Set<String> stores = keyValueStoreService.getStateStores();

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
            () -> keyValueStoreService.getStreamsMetadataForStore("store"));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetStreamsMetadataForStore() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        Collection<StreamsMetadata> streamsMetadataResponse = keyValueStoreService
            .getStreamsMetadataForStore("store");

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
            () -> keyValueStoreService.getAll("store"));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetAllThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(null);

        assertThrows(UnknownStateStoreException.class, () -> keyValueStoreService.getAll("store"));
    }

    @Test
    void shouldGetAllThrowsUnknownStoreExceptionWhenMetadataEmpty() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(Collections.emptyList());

        assertThrows(UnknownStateStoreException.class, () -> keyValueStoreService.getAll("store"));
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
        when(stateRangeQueryResult.getPartitionResults()).thenReturn(Map.of(0, queryResult));
        doCallRealMethod().when(iterator).forEachRemaining(any());
        when(iterator.hasNext())
            .thenReturn(true)
            .thenReturn(false);

        PersonStub personStub = new PersonStub("John", "Doe");
        when(iterator.next())
            .thenReturn(KeyValue.pair("key", ValueAndTimestamp.make(personStub, 150L)));

        List<StateStoreRecord> responses = keyValueStoreService.getAll("store");

        assertEquals("key", responses.get(0).getKey());
        assertEquals("John", ((Map<?, ?>) responses.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) responses.get(0).getValue()).get("lastName"));
        assertEquals(150L, responses.get(0).getTimestamp());
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
                "timestamp": 150
              }
            ]""");

        List<StateStoreRecord> responses = keyValueStoreService.getAll("store");

        assertEquals("key", responses.get(0).getKey());
        assertEquals("John", ((Map<?, ?>) responses.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) responses.get(0).getValue()).get("lastName"));
        assertEquals(150L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetAllOnLocalhostThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(null);

        assertThrows(UnknownStateStoreException.class, () -> keyValueStoreService.getAllOnLocalhost("store"));
    }

    @Test
    void shouldGetAllOnLocalhostThrowsUnknownStoreExceptionWhenMetadataEmpty() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(Collections.emptyList());

        assertThrows(UnknownStateStoreException.class, () -> keyValueStoreService.getAllOnLocalhost("store"));
    }

    @Test
    void shouldGetAllOnLocalhost() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<KeyValueIterator<Object,
            ValueAndTimestamp<Object>>>>any())).thenReturn(stateRangeQueryResult);

        QueryResult<KeyValueIterator<Object, ValueAndTimestamp<Object>>> queryResult = QueryResult.forResult(iterator);
        when(stateRangeQueryResult.getPartitionResults()).thenReturn(Map.of(0, queryResult));
        doCallRealMethod().when(iterator).forEachRemaining(any());
        when(iterator.hasNext())
            .thenReturn(true)
            .thenReturn(false);

        PersonStub personStub = new PersonStub("John", "Doe");
        when(iterator.next())
            .thenReturn(KeyValue.pair("key", ValueAndTimestamp.make(personStub, 150L)));

        List<StateStoreRecord> responses = keyValueStoreService.getAllOnLocalhost("store");

        assertEquals("key", responses.get(0).getKey());
        assertEquals("John", ((Map<?, ?>) responses.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) responses.get(0).getValue()).get("lastName"));
        assertEquals(150L, responses.get(0).getTimestamp());
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
            () -> keyValueStoreService.getAll("store"));

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

        StreamsNotStartedException exception = assertThrows(StreamsNotStartedException.class,
            () -> keyValueStoreService.getByKey("store", "key"));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetByKeyThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(null);

        assertThrows(UnknownStateStoreException.class, () ->
            keyValueStoreService.getByKey("store", "key"));
    }

    @Test
    void shouldGetByKeyCurrentInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(new KeyQueryMetadata(
                new HostInfo("localhost", 8080),
                Collections.emptySet(),
                0)
            );

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(hostInfo);

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<ValueAndTimestamp<Object>>>any()))
            .thenReturn(stateKeyQueryResult);

        QueryResult<ValueAndTimestamp<Object>> queryResult = QueryResult
            .forResult(ValueAndTimestamp.make(new PersonStub("John", "Doe"), 150L));

        when(stateKeyQueryResult.getOnlyPartitionResult())
            .thenReturn(queryResult);

        StateStoreRecord response = keyValueStoreService.getByKey("store", "key");

        assertEquals("key", response.getKey());
        assertEquals("John", ((Map<?, ?>) response.getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) response.getValue()).get("lastName"));
        assertEquals(150L, response.getTimestamp());
    }

    @Test
    void shouldGetByKeyOtherInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(new KeyQueryMetadata(
                new HostInfo("localhost", 8085),
                Collections.emptySet(),
                0)
            );

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
                "timestamp": 150
              }
            """);

        StateStoreRecord response = keyValueStoreService.getByKey("store", "key");

        assertEquals("key", response.getKey());
        assertEquals("John", ((Map<?, ?>) response.getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) response.getValue()).get("lastName"));
        assertEquals(150L, response.getTimestamp());
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

        when(stateKeyQueryResult.getOnlyPartitionResult())
            .thenReturn(null);

        UnknownKeyException exception = assertThrows(UnknownKeyException.class, () ->
            keyValueStoreService.getByKey("store", "unknownKey"));

        assertEquals("Key unknownKey not found", exception.getMessage());
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

        OtherInstanceResponseException exception = assertThrows(OtherInstanceResponseException.class,
            () -> keyValueStoreService.getByKey("store", "key"));

        assertEquals("Fail to read other instance response", exception.getMessage());
    }

    record PersonStub(String firstName, String lastName) { }
}
