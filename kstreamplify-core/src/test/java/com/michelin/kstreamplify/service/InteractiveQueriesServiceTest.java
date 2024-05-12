package com.michelin.kstreamplify.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.model.QueryResponse;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetadata;
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
    void shouldGetStreamsMetadataForStore() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        Collection<StreamsMetadata> streamsMetadataResponse = interactiveQueriesService.getStreamsMetadata("store");

        assertIterableEquals(List.of(streamsMetadata), streamsMetadataResponse);
    }

    @Test
    void shouldGetAllThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(null);

        assertThrows(UnknownStateStoreException.class, () -> interactiveQueriesService.getAll("store", false, false));
    }

    @Test
    void shouldGetAllThrowsUnknownStoreExceptionWhenMetadataEmpty() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(Collections.emptyList());

        assertThrows(UnknownStateStoreException.class, () -> interactiveQueriesService.getAll("store", false, false));
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
        when(stateRangeQueryResult.getPartitionResults()).thenReturn(Map.of(0, QueryResult.forResult(iterator)));
        doCallRealMethod().when(iterator).forEachRemaining(any());
        when(iterator.hasNext())
            .thenReturn(true)
            .thenReturn(false);

        when(iterator.next())
            .thenReturn(KeyValue.pair("key", ValueAndTimestamp.make(new PersonTest("John", "Doe"), 150L)));

        List<QueryResponse> responses = interactiveQueriesService.getAll("store", false, false);

        assertNull(responses.get(0).getKey());
        assertEquals(Map.of("firstName", "John", "lastName", "Doe"), responses.get(0).getValue());
        assertNull(responses.get(0).getTimestamp());
        assertNull(responses.get(0).getHostInfo());
        assertNull(responses.get(0).getPositionVectors());
    }

    @Test
    void shouldGetAllCurrentInstanceIncludeAll() {
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

        when(iterator.next())
            .thenReturn(KeyValue.pair("key", ValueAndTimestamp.make(new PersonTest("John", "Doe"), 150L)));

        List<QueryResponse> responses = interactiveQueriesService.getAll("store", true, true);

        assertEquals("key", responses.get(0).getKey());
        assertEquals(Map.of("firstName", "John", "lastName", "Doe"), responses.get(0).getValue());
        assertEquals(150L, responses.get(0).getTimestamp());
        assertEquals("localhost", responses.get(0).getHostInfo().host());
        assertEquals(8080, responses.get(0).getHostInfo().port());
        assertEquals("topic", responses.get(0).getPositionVectors().get(0).getTopic());
        assertEquals(0, responses.get(0).getPositionVectors().get(0).getPartition());
        assertEquals(15L, responses.get(0).getPositionVectors().get(0).getOffset());
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
                "value": {
                  "firstName": "John",
                  "lastName": "Doe"
                }
              }
            ]""");

        List<QueryResponse> responses = interactiveQueriesService.getAll("store", false, false);

        assertNull(responses.get(0).getKey());
        assertEquals(Map.of("firstName", "John", "lastName", "Doe"), responses.get(0).getValue());
        assertNull(responses.get(0).getTimestamp());
        assertNull(responses.get(0).getHostInfo());
        assertNull(responses.get(0).getPositionVectors());
    }

    @Test
    void shouldGetAllOtherInstanceIncludeAll() {
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

        List<QueryResponse> responses = interactiveQueriesService.getAll("store", true, true);

        assertEquals("key", responses.get(0).getKey());
        assertEquals(Map.of("firstName", "John", "lastName", "Doe"), responses.get(0).getValue());
        assertEquals(150L, responses.get(0).getTimestamp());
        assertEquals("localhost", responses.get(0).getHostInfo().host());
        assertEquals(8080, responses.get(0).getHostInfo().port());
        assertEquals("topic", responses.get(0).getPositionVectors().get(0).getTopic());
        assertEquals(0, responses.get(0).getPositionVectors().get(0).getPartition());
        assertEquals(15L, responses.get(0).getPositionVectors().get(0).getOffset());
    }

    @Test
    void shouldGetByKeyThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(null);

        assertThrows(UnknownStateStoreException.class, () ->
            interactiveQueriesService.getByKey("store", "key", new StringSerializer(), false, false));
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
        when(stateKeyQueryResult.getOnlyPartitionResult())
            .thenReturn(QueryResult.forResult(ValueAndTimestamp.make(new PersonTest("John", "Doe"), 150L)));

        QueryResponse response = interactiveQueriesService
            .getByKey("store", "key", new StringSerializer(), false, false);

        assertNull(response.getKey());
        assertEquals(Map.of("firstName", "John", "lastName", "Doe"), response.getValue());
        assertNull(response.getTimestamp());
        assertNull(response.getHostInfo());
        assertNull(response.getPositionVectors());
    }

    @Test
    void shouldGetByKeyCurrentInstanceIncludeAll() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);
        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
            .thenReturn(new KeyQueryMetadata(new HostInfo("localhost", 8080), Collections.emptySet(), 0));

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(hostInfo);

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<ValueAndTimestamp<Object>>>any()))
            .thenReturn(stateKeyQueryResult);

        QueryResult<ValueAndTimestamp<Object>> queryResult = QueryResult
            .forResult(ValueAndTimestamp.make(new PersonTest("John", "Doe"), 150L));
        queryResult.setPosition(Position.fromMap(Map.of("topic", Map.of(0, 15L))));

        when(stateKeyQueryResult.getOnlyPartitionResult())
            .thenReturn(queryResult);

        QueryResponse response = interactiveQueriesService
            .getByKey("store", "key", new StringSerializer(), true, true);

        assertEquals("key", response.getKey());
        assertEquals(Map.of("firstName", "John", "lastName", "Doe"), response.getValue());
        assertEquals(150L, response.getTimestamp());
        assertEquals("localhost", response.getHostInfo().host());
        assertEquals(8080, response.getHostInfo().port());
        assertEquals("topic", response.getPositionVectors().get(0).getTopic());
        assertEquals(0, response.getPositionVectors().get(0).getPartition());
        assertEquals(15L, response.getPositionVectors().get(0).getOffset());
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
                "value": {
                  "firstName": "John",
                  "lastName": "Doe"
                }
              }
            """);

        QueryResponse response = interactiveQueriesService
            .getByKey("store", "key", new StringSerializer(), true, true);

        assertNull(response.getKey());
        assertEquals(Map.of("firstName", "John", "lastName", "Doe"), response.getValue());
        assertNull(response.getTimestamp());
        assertNull(response.getHostInfo());
        assertNull(response.getPositionVectors());
    }

    @Test
    void shouldGetByKeyOtherInstanceIncludeAll() {
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

        QueryResponse response = interactiveQueriesService
            .getByKey("store", "key", new StringSerializer(), true, true);

        assertEquals("key", response.getKey());
        assertEquals(Map.of("firstName", "John", "lastName", "Doe"), response.getValue());
        assertEquals(150L, response.getTimestamp());
        assertEquals("localhost", response.getHostInfo().host());
        assertEquals(8080, response.getHostInfo().port());
        assertEquals("topic", response.getPositionVectors().get(0).getTopic());
        assertEquals(0, response.getPositionVectors().get(0).getPartition());
        assertEquals(15L, response.getPositionVectors().get(0).getOffset());
    }

    @Getter
    @AllArgsConstructor
    static class PersonTest {
        private String firstName;
        private String lastName;
    }
}
