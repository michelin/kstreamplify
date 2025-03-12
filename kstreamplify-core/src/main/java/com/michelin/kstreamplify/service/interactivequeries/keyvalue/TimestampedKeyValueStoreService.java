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
package com.michelin.kstreamplify.service.interactivequeries.keyvalue;

import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.TimestampedKeyQuery;
import org.apache.kafka.streams.query.TimestampedRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/** Timestamped key-value store service. */
@Slf4j
public class TimestampedKeyValueStoreService extends CommonKeyValueStoreService {

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    public TimestampedKeyValueStoreService(KafkaStreamsInitializer kafkaStreamsInitializer) {
        super(kafkaStreamsInitializer);
    }

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @param httpClient The HTTP client
     */
    @SuppressWarnings("unused")
    public TimestampedKeyValueStoreService(KafkaStreamsInitializer kafkaStreamsInitializer, HttpClient httpClient) {
        super(httpClient, kafkaStreamsInitializer);
    }

    /** {@inheritDoc} */
    @Override
    protected String path() {
        return "key-value/timestamped";
    }

    /** {@inheritDoc} */
    @Override
    protected List<StateStoreRecord> executeRangeQuery(String store) {
        TimestampedRangeQuery<String, Object> rangeQuery = TimestampedRangeQuery.withNoBounds();
        StateQueryResult<KeyValueIterator<String, ValueAndTimestamp<Object>>> result = kafkaStreamsInitializer
                .getKafkaStreams()
                .query(StateQueryRequest.inStore(store).withQuery(rangeQuery));

        List<StateStoreRecord> partitionsResult = new ArrayList<>();
        result.getPartitionResults().forEach((key, queryResult) -> queryResult
                .getResult()
                .forEachRemaining(kv ->
                        partitionsResult.add(new StateStoreRecord(kv.key, kv.value.value(), kv.value.timestamp()))));

        return new ArrayList<>(partitionsResult);
    }

    /** {@inheritDoc} */
    @Override
    protected StateStoreRecord executeKeyQuery(KeyQueryMetadata keyQueryMetadata, String store, String key) {
        TimestampedKeyQuery<String, Object> keyQuery = TimestampedKeyQuery.withKey(key);
        StateQueryResult<ValueAndTimestamp<Object>> result = kafkaStreamsInitializer
                .getKafkaStreams()
                .query(StateQueryRequest.inStore(store)
                        .withQuery(keyQuery)
                        .withPartitions(Collections.singleton(keyQueryMetadata.partition())));

        if (result.getPartitionResults().values().stream().anyMatch(QueryResult::isFailure)) {
            throw new IllegalArgumentException(
                    result.getPartitionResults().get(0).getFailureMessage());
        }

        if (result.getOnlyPartitionResult() == null) {
            throw new UnknownKeyException(key);
        }

        return new StateStoreRecord(
                key,
                result.getOnlyPartitionResult().getResult().value(),
                result.getOnlyPartitionResult().getResult().timestamp());
    }
}
