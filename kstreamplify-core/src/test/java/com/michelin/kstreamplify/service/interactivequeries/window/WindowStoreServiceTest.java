/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.service.interactivequeries.window;

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

import com.michelin.kstreamplify.exception.OtherInstanceResponseException;
import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Instant;
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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WindowStoreServiceTest {
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
    private StateQueryResult<KeyValueIterator<Windowed<String>, Object>> stateWindowRangeQueryResult;

    @Mock
    private KeyValueIterator<Windowed<String>, Object> iterator;

    @Mock
    private StateQueryResult<WindowStoreIterator<Object>> stateWindowKeyQueryResult;

    @Mock
    private WindowStoreIterator<Object> windowStoreIterator;

    @Mock
    private HttpResponse<String> httpResponse;

    @InjectMocks
    private WindowStoreService windowStoreService;

    @Test
    void shouldValidatePath() {
        assertEquals("window", windowStoreService.path());
    }

    @Test
    void shouldNotGetStoresWhenStreamsIsNotStarted() {
        when(kafkaStreamsInitializer.isNotRunning()).thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);

        StreamsNotStartedException exception =
                assertThrows(StreamsNotStartedException.class, () -> windowStoreService.getStateStores());

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetStores() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(List.of(streamsMetadata));

        when(streamsMetadata.stateStoreNames()).thenReturn(Set.of("store1", "store2"));

        Set<String> stores = windowStoreService.getStateStores();

        assertTrue(stores.contains("store1"));
        assertTrue(stores.contains("store2"));
    }

    @Test
    void shouldGetStoresWhenNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(null);

        Set<String> stores = windowStoreService.getStateStores();

        assertTrue(stores.isEmpty());
    }

    @Test
    void shouldGetStoresWhenEmpty() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.metadataForAllStreamsClients()).thenReturn(Collections.emptyList());

        Set<String> stores = windowStoreService.getStateStores();

        assertTrue(stores.isEmpty());
    }

    @Test
    void shouldNotGetStreamsMetadataForStoreWhenStreamsIsNotStarted() {
        when(kafkaStreamsInitializer.isNotRunning()).thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);

        StreamsNotStartedException exception = assertThrows(
                StreamsNotStartedException.class, () -> windowStoreService.getStreamsMetadataForStore("store"));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetStreamsMetadataForStore() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        Collection<StreamsMetadata> streamsMetadataResponse = windowStoreService.getStreamsMetadataForStore("store");

        assertIterableEquals(List.of(streamsMetadata), streamsMetadataResponse);
    }

    @Test
    void shouldNotGetAllWhenStreamsIsNotStarted() {
        when(kafkaStreamsInitializer.isNotRunning()).thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);

        Instant instant = Instant.now();
        StreamsNotStartedException exception = assertThrows(
                StreamsNotStartedException.class, () -> windowStoreService.getAll("store", instant, instant));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetAllThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(null);

        Instant instant = Instant.now();
        assertThrows(UnknownStateStoreException.class, () -> windowStoreService.getAll("store", instant, instant));
    }

    @Test
    void shouldGetAllThrowsUnknownStoreExceptionWhenMetadataEmpty() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(Collections.emptyList());

        Instant instant = Instant.now();
        assertThrows(UnknownStateStoreException.class, () -> windowStoreService.getAll("store", instant, instant));
    }

    @Test
    void shouldGetAll() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        HostInfo hostInfo = new HostInfo("localhost", 8080);
        when(streamsMetadata.hostInfo()).thenReturn(hostInfo);

        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(hostInfo);

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<KeyValueIterator<Windowed<String>, Object>>>any()))
                .thenReturn(stateWindowRangeQueryResult);

        when(stateWindowRangeQueryResult.getPartitionResults()).thenReturn(Map.of(0, QueryResult.forResult(iterator)));

        doCallRealMethod().when(iterator).forEachRemaining(any());
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);

        when(iterator.next())
                .thenReturn(
                        KeyValue.pair(new Windowed<>("key", new TimeWindow(0L, 150L)), new UserStub("John", "Doe")));

        List<StateStoreRecord> responses = windowStoreService.getAll("store", Instant.EPOCH, Instant.now());

        assertEquals("key", responses.get(0).getKey());
        assertEquals("John", ((Map<?, ?>) responses.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) responses.get(0).getValue()).get("lastName"));
        assertNull(responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetAllWithRemoteCall() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        when(streamsMetadata.hostInfo()).thenReturn(new HostInfo("localhost", 8080));

        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(new HostInfo("anotherHost", 8080));

        when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofString())))
                .thenReturn(CompletableFuture.completedFuture(httpResponse));

        when(httpResponse.body())
                .thenReturn(
                        """
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

        List<StateStoreRecord> responses = windowStoreService.getAll("store", Instant.EPOCH, Instant.now());

        assertEquals("key", responses.get(0).getKey());
        assertEquals("John", ((Map<?, ?>) responses.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) responses.get(0).getValue()).get("lastName"));
        assertEquals(150L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetAllOnLocalHostThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(null);

        Instant instant = Instant.now();
        assertThrows(
                UnknownStateStoreException.class,
                () -> windowStoreService.getAllOnLocalInstance("store", instant, instant));
    }

    @Test
    void shouldGetAllOnLocalHostThrowsUnknownStoreExceptionWhenMetadataEmpty() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(Collections.emptyList());

        Instant instant = Instant.now();
        assertThrows(
                UnknownStateStoreException.class,
                () -> windowStoreService.getAllOnLocalInstance("store", instant, instant));
    }

    @Test
    void shouldGetAllOnLocalHost() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<KeyValueIterator<Windowed<String>, Object>>>any()))
                .thenReturn(stateWindowRangeQueryResult);

        when(stateWindowRangeQueryResult.getPartitionResults()).thenReturn(Map.of(0, QueryResult.forResult(iterator)));

        doCallRealMethod().when(iterator).forEachRemaining(any());
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);

        when(iterator.next())
                .thenReturn(
                        KeyValue.pair(new Windowed<>("key", new TimeWindow(0L, 150L)), new UserStub("John", "Doe")));

        Instant instant = Instant.now();
        List<StateStoreRecord> responses = windowStoreService.getAllOnLocalInstance("store", instant, instant);

        assertEquals("key", responses.get(0).getKey());
        assertEquals("John", ((Map<?, ?>) responses.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) responses.get(0).getValue()).get("lastName"));
        assertNull(responses.get(0).getTimestamp());
    }

    @Test
    void shouldHandleRuntimeExceptionWhenGettingAllOtherInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.streamsMetadataForStore(any())).thenReturn(List.of(streamsMetadata));

        when(streamsMetadata.hostInfo()).thenReturn(new HostInfo("localhost", 8080));

        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(new HostInfo("anotherHost", 8080));

        when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofString())))
                .thenThrow(new RuntimeException("Error"));

        Instant instant = Instant.now();
        OtherInstanceResponseException exception = assertThrows(
                OtherInstanceResponseException.class, () -> windowStoreService.getAll("store", instant, instant));

        assertEquals("Fail to read other instance response", exception.getMessage());
    }

    @Test
    void shouldNotGetByKeyWhenStreamsIsNotStarted() {
        when(kafkaStreamsInitializer.isNotRunning()).thenReturn(true);

        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);

        Instant instant = Instant.now();
        StreamsNotStartedException exception = assertThrows(
                StreamsNotStartedException.class, () -> windowStoreService.getByKey("store", "key", instant, instant));

        assertEquals(STREAMS_NOT_STARTED, exception.getMessage());
    }

    @Test
    void shouldGetByKeyThrowsUnknownStoreExceptionWhenMetadataNull() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
                .thenReturn(null);

        Instant instant = Instant.now();
        assertThrows(
                UnknownStateStoreException.class, () -> windowStoreService.getByKey("store", "key", instant, instant));
    }

    @Test
    void shouldGetByKeyCurrentInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
                .thenReturn(new KeyQueryMetadata(new HostInfo("localhost", 8080), Collections.emptySet(), 0));

        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(new HostInfo("localhost", 8080));

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<WindowStoreIterator<Object>>>any()))
                .thenReturn(stateWindowKeyQueryResult);

        when(stateWindowKeyQueryResult.getOnlyPartitionResult()).thenReturn(QueryResult.forResult(windowStoreIterator));

        doCallRealMethod().when(windowStoreIterator).forEachRemaining(any());
        when(windowStoreIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);

        when(windowStoreIterator.next()).thenReturn(KeyValue.pair(0L, new UserStub("John", "Doe")));

        List<StateStoreRecord> responses = windowStoreService.getByKey("store", "key", Instant.EPOCH, Instant.now());

        assertEquals("key", responses.get(0).getKey());
        assertEquals("John", ((Map<?, ?>) responses.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) responses.get(0).getValue()).get("lastName"));
        assertNull(responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetByKeyOtherInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
                .thenReturn(new KeyQueryMetadata(new HostInfo("localhost", 8085), Collections.emptySet(), 0));

        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(new HostInfo("localhost", 8080));

        when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofString())))
                .thenReturn(CompletableFuture.completedFuture(httpResponse));

        when(httpResponse.body())
                .thenReturn(
                        """
              [
                {
                  "key": "key",
                  "value": {
                    "firstName": "John",
                    "lastName": "Doe"
                  },
                  "timestamp": 150
                }
              ]
            """);

        List<StateStoreRecord> responses = windowStoreService.getByKey("store", "key", Instant.EPOCH, Instant.now());

        assertEquals("key", responses.get(0).getKey());
        assertEquals("John", ((Map<?, ?>) responses.get(0).getValue()).get("firstName"));
        assertEquals("Doe", ((Map<?, ?>) responses.get(0).getValue()).get("lastName"));
        assertEquals(150L, responses.get(0).getTimestamp());
    }

    @Test
    void shouldGetUnknownKeyCurrentInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
                .thenReturn(new KeyQueryMetadata(new HostInfo("localhost", 8080), Collections.emptySet(), 0));

        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(new HostInfo("localhost", 8080));

        when(kafkaStreams.query(ArgumentMatchers.<StateQueryRequest<WindowStoreIterator<Object>>>any()))
                .thenReturn(stateWindowKeyQueryResult);

        when(stateWindowKeyQueryResult.getOnlyPartitionResult()).thenReturn(QueryResult.forResult(windowStoreIterator));

        Instant instant = Instant.now();
        UnknownKeyException exception = assertThrows(
                UnknownKeyException.class, () -> windowStoreService.getByKey("store", "unknownKey", instant, instant));

        assertEquals("Key unknownKey not found", exception.getMessage());
    }

    @Test
    void shouldHandleRuntimeExceptionWhenGettingByKeyOtherInstance() {
        when(kafkaStreamsInitializer.getKafkaStreams()).thenReturn(kafkaStreams);

        when(kafkaStreams.queryMetadataForKey(anyString(), any(), ArgumentMatchers.<Serializer<Object>>any()))
                .thenReturn(new KeyQueryMetadata(new HostInfo("localhost", 8085), Collections.emptySet(), 0));

        when(kafkaStreamsInitializer.getHostInfo()).thenReturn(new HostInfo("localhost", 8080));

        when(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofString())))
                .thenThrow(new RuntimeException("Error"));

        Instant instant = Instant.now();
        OtherInstanceResponseException exception = assertThrows(
                OtherInstanceResponseException.class,
                () -> windowStoreService.getByKey("store", "key", instant, instant));

        assertEquals("Fail to read other instance response", exception.getMessage());
    }

    record UserStub(String firstName, String lastName) {}
}
