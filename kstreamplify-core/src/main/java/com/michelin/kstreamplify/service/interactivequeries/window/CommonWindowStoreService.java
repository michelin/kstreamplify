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

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.service.interactivequeries.CommonStoreService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.state.HostInfo;

@Slf4j
abstract class CommonWindowStoreService extends CommonStoreService {
    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    protected CommonWindowStoreService(KafkaStreamsInitializer kafkaStreamsInitializer) {
        super(kafkaStreamsInitializer);
    }

    /**
     * Constructor.
     *
     * @param httpClient The HTTP client
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    protected CommonWindowStoreService(HttpClient httpClient, KafkaStreamsInitializer kafkaStreamsInitializer) {
        super(httpClient, kafkaStreamsInitializer);
    }

    /**
     * Get all values from the store.
     *
     * @param store The store
     * @param startTime The start time
     * @param endTime The end time
     * @return The values
     */
    public List<StateStoreRecord> getAll(String store, Instant startTime, Instant endTime) {
        final Collection<StreamsMetadata> streamsMetadata = getStreamsMetadataForStore(store);

        if (streamsMetadata == null || streamsMetadata.isEmpty()) {
            throw new UnknownStateStoreException(UNKNOWN_STATE_STORE.formatted(store));
        }

        List<StateStoreRecord> results = new ArrayList<>();
        streamsMetadata.forEach(metadata -> {
            if (isNotCurrentHost(metadata.hostInfo())) {
                log.debug("Fetching data on other instance ({}:{})", metadata.host(), metadata.port());

                results.addAll(getAllOnRemoteHost(
                        metadata.hostInfo(),
                        "store/" + path() + "/local/" + store + "?startTime=" + startTime + "&endTime=" + endTime));
            } else {
                log.debug("Fetching data on this instance ({}:{})", metadata.host(), metadata.port());

                results.addAll(executeWindowRangeQuery(store, startTime, endTime));
            }
        });

        return results;
    }

    /**
     * Get the value by key from the store.
     *
     * @param store The store name
     * @param key The key
     * @param startTime The start time
     * @param endTime The end time
     * @return The value
     */
    public List<StateStoreRecord> getByKey(String store, String key, Instant startTime, Instant endTime) {
        KeyQueryMetadata keyQueryMetadata = getKeyQueryMetadata(store, key, new StringSerializer());

        if (keyQueryMetadata == null) {
            throw new UnknownStateStoreException(UNKNOWN_STATE_STORE.formatted(store));
        }

        HostInfo host = keyQueryMetadata.activeHost();
        if (isNotCurrentHost(host)) {
            log.debug("The key {} has been located on another instance ({}:{})", key, host.host(), host.port());

            return getAllOnRemoteHost(
                    host,
                    "store/" + path() + "/" + store + "/" + key + "?startTime=" + startTime + "&endTime=" + endTime);
        }

        log.debug("The key {} has been located on the current instance ({}:{})", key, host.host(), host.port());

        return executeWindowKeyQuery(keyQueryMetadata, store, key, startTime, endTime);
    }

    /**
     * Get all values from the store on the local instance.
     *
     * @param store The store
     * @param startTime The start time
     * @param endTime The end time
     * @return The values
     */
    public List<StateStoreRecord> getAllOnLocalInstance(String store, Instant startTime, Instant endTime) {
        final Collection<StreamsMetadata> streamsMetadata = getStreamsMetadataForStore(store);

        if (streamsMetadata == null || streamsMetadata.isEmpty()) {
            throw new UnknownStateStoreException(UNKNOWN_STATE_STORE.formatted(store));
        }

        return executeWindowRangeQuery(store, startTime, endTime);
    }

    /**
     * Execute a window range query on the store.
     *
     * @param store The store
     * @param startTime The start time
     * @param endTime The end time
     * @return The values
     */
    protected abstract List<StateStoreRecord> executeWindowRangeQuery(String store, Instant startTime, Instant endTime);

    /**
     * Execute a window key query on the store.
     *
     * @param keyQueryMetadata The key query metadata
     * @param store The store
     * @param key The key
     * @param startTime The start time
     * @param endTime The end time
     * @return The values
     */
    protected abstract List<StateStoreRecord> executeWindowKeyQuery(
            KeyQueryMetadata keyQueryMetadata, String store, String key, Instant startTime, Instant endTime);
}
