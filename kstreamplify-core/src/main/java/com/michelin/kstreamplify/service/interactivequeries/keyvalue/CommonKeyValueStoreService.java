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

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.service.interactivequeries.CommonStoreService;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.http.HttpClient;
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
abstract class CommonKeyValueStoreService extends CommonStoreService {
    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    protected CommonKeyValueStoreService(KafkaStreamsInitializer kafkaStreamsInitializer) {
        super(kafkaStreamsInitializer);
    }

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @param httpClient              The HTTP client
     */
    protected CommonKeyValueStoreService(HttpClient httpClient,
                                         KafkaStreamsInitializer kafkaStreamsInitializer) {
        super(httpClient, kafkaStreamsInitializer);
    }

    /**
     * Get all values from the store.
     *
     * @param store The store
     * @return The values
     */
    public List<StateStoreRecord> getAll(String store) {
        final Collection<StreamsMetadata> streamsMetadata = getStreamsMetadataForStore(store);

        if (streamsMetadata == null || streamsMetadata.isEmpty()) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        List<StateStoreRecord> results = new ArrayList<>();
        streamsMetadata.forEach(metadata -> {
            if (isNotCurrentHost(metadata.hostInfo())) {
                log.debug("Fetching data on other instance ({}:{})", metadata.host(), metadata.port());

                results.addAll(
                    getAllOnRemoteHost(metadata.hostInfo(), "store/" + path() + "/local/" + store)
                );
            } else {
                log.debug("Fetching data on this instance ({}:{})", metadata.host(), metadata.port());

                results.addAll(executeRangeQuery(store));
            }
        });

        return results;
    }

    /**
     * Get the value by key from the store.
     *
     * @param store The store name
     * @param key   The key
     * @return The value
     */
    public StateStoreRecord getByKey(String store, String key) {
        KeyQueryMetadata keyQueryMetadata = getKeyQueryMetadata(store, key, new StringSerializer());

        if (keyQueryMetadata == null) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        HostInfo host = keyQueryMetadata.activeHost();
        if (isNotCurrentHost(host)) {
            log.debug("The key {} has been located on another instance ({}:{})", key,
                host.host(), host.port());

            return getByKeyOnRemoteHost(host, "store/" + path() + "/" + store + "/" + key);
        }

        log.debug("The key {} has been located on the current instance ({}:{})", key,
            host.host(), host.port());

        return executeKeyQuery(keyQueryMetadata, store, key);
    }

    /**
     * Get all values from the store on the local host.
     *
     * @param store The store
     * @return The values
     */
    public List<StateStoreRecord> getAllOnLocalHost(String store) {
        final Collection<StreamsMetadata> streamsMetadata = getStreamsMetadataForStore(store);

        if (streamsMetadata == null || streamsMetadata.isEmpty()) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        return executeRangeQuery(store);
    }

    protected abstract List<StateStoreRecord> executeRangeQuery(String store);

    protected abstract StateStoreRecord executeKeyQuery(KeyQueryMetadata keyQueryMetadata, String store, String key);
}
