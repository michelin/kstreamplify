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

import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Window store service.
 */
@Slf4j
public class TimestampedWindowStoreService extends CommonWindowStoreService {

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    public TimestampedWindowStoreService(KafkaStreamsInitializer kafkaStreamsInitializer) {
        super(kafkaStreamsInitializer);
    }

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @param httpClient              The HTTP client
     */
    @SuppressWarnings("unused")
    public TimestampedWindowStoreService(KafkaStreamsInitializer kafkaStreamsInitializer, HttpClient httpClient) {
        super(httpClient, kafkaStreamsInitializer);
    }

    /**
     * Execute a window range query.
     *
     * @param store   The store
     * @param timeFrom The time from
     * @param timeTo  The time to
     * @return The results
     */
    @Override
    protected List<StateStoreRecord> executeWindowRangeQuery(String store, Instant timeFrom, Instant timeTo) {
        WindowRangeQuery<String, ValueAndTimestamp<Object>> windowRangeQuery = WindowRangeQuery
            .withWindowStartRange(timeFrom, timeTo);

        StateQueryResult<KeyValueIterator<Windowed<String>, ValueAndTimestamp<Object>>> result = kafkaStreamsInitializer
            .getKafkaStreams()
            .query(StateQueryRequest
                .inStore(store)
                .withQuery(windowRangeQuery));

        List<StateStoreRecord> partitionsResult = new ArrayList<>();
        result.getPartitionResults().forEach((key, queryResult) ->
            queryResult.getResult().forEachRemaining(kv -> partitionsResult.add(
                new StateStoreRecord(
                    kv.key.key(),
                    kv.value.value(),
                    kv.value.timestamp()
                )
            )));

        return partitionsResult;
    }

    /**
     * Execute a window key query.
     *
     * @param keyQueryMetadata The key query metadata
     * @param store The store
     * @param key The key
     * @param timeFrom  The time from
     * @param timeTo The time to
     * @return The results
     */
    @Override
    protected List<StateStoreRecord> executeKeyQuery(KeyQueryMetadata keyQueryMetadata,
                                                   String store,
                                                   String key,
                                                   Instant timeFrom,
                                                   Instant timeTo) {
        WindowKeyQuery<String, ValueAndTimestamp<Object>> windowKeyQuery = WindowKeyQuery
            .withKeyAndWindowStartRange(key, timeFrom, timeTo);

        StateQueryResult<WindowStoreIterator<ValueAndTimestamp<Object>>> result = kafkaStreamsInitializer
            .getKafkaStreams()
            .query(StateQueryRequest
                .inStore(store)
                .withQuery(windowKeyQuery)
                .withPartitions(Collections.singleton(keyQueryMetadata.partition())));

        if (!result.getOnlyPartitionResult().getResult().hasNext()) {
            throw new UnknownKeyException(key);
        }

        List<StateStoreRecord> partitionsResult = new ArrayList<>();
        result.getOnlyPartitionResult().getResult().forEachRemaining(kv -> partitionsResult.add(
            new StateStoreRecord(key, kv.value.value(), kv.value.timestamp())
        ));

        return partitionsResult;
    }
}
