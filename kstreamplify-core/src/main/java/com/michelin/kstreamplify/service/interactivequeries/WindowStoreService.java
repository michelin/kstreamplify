package com.michelin.kstreamplify.service.interactivequeries;

import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Window store service.
 */
@Slf4j
public class WindowStoreService extends InteractiveQueriesService {

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    public WindowStoreService(KafkaStreamsInitializer kafkaStreamsInitializer) {
        super(kafkaStreamsInitializer);
    }

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @param httpClient The HTTP client
     */
    @SuppressWarnings("unused")
    public WindowStoreService(KafkaStreamsInitializer kafkaStreamsInitializer, HttpClient httpClient) {
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
                    getAllOnOtherHost(metadata.hostInfo(), "store/window/local/" + store)
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
    public List<StateStoreRecord> getByKey(String store, String key) {
        KeyQueryMetadata keyQueryMetadata = getKeyQueryMetadata(store, key, new StringSerializer());

        if (keyQueryMetadata == null) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        HostInfo host = keyQueryMetadata.activeHost();
        if (isNotCurrentHost(host)) {
            log.debug("The key {} has been located on another instance ({}:{})", key,
                host.host(), host.port());

            return getAllOnOtherHost(host, "store/window/" + store + "/" + key);
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
    public List<StateStoreRecord> getAllOnLocalhost(String store) {
        final Collection<StreamsMetadata> streamsMetadata = getStreamsMetadataForStore(store);

        if (streamsMetadata == null || streamsMetadata.isEmpty()) {
            throw new UnknownStateStoreException(String.format(UNKNOWN_STATE_STORE, store));
        }

        return executeRangeQuery(store);
    }

    @SuppressWarnings("unchecked")
    private List<StateStoreRecord> executeRangeQuery(String store) {
        WindowRangeQuery<String, Object> windowRangeQuery = WindowRangeQuery
            .withWindowStartRange(Instant.EPOCH, Instant.now());

        StateQueryResult<KeyValueIterator<Windowed<String>, Object>> result = kafkaStreamsInitializer
            .getKafkaStreams()
            .query(StateQueryRequest
                .inStore(store)
                .withQuery(windowRangeQuery));

        List<StateStoreRecord> partitionsResult = new ArrayList<>();
        result.getPartitionResults().forEach((key, queryResult) ->
            queryResult.getResult().forEachRemaining(kv -> {
                if (kv.value instanceof ValueAndTimestamp<?>) {
                    ValueAndTimestamp<Object> valueAndTimestamp = (ValueAndTimestamp<Object>) kv.value;

                    partitionsResult.add(
                        new StateStoreRecord(
                            kv.key.key(),
                            valueAndTimestamp.value(),
                            valueAndTimestamp.timestamp()
                        )
                    );
                } else {
                    partitionsResult.add(new StateStoreRecord(kv.key.key(), kv.value));
                }
            }));

        return partitionsResult;
    }

    @SuppressWarnings("unchecked")
    private List<StateStoreRecord> executeKeyQuery(KeyQueryMetadata keyQueryMetadata, String store, String key) {
        WindowKeyQuery<String, Object> windowKeyQuery = WindowKeyQuery
            .withKeyAndWindowStartRange(key, Instant.EPOCH, Instant.now());

        StateQueryResult<WindowStoreIterator<Object>> result = kafkaStreamsInitializer
            .getKafkaStreams()
            .query(StateQueryRequest
                .inStore(store)
                .withQuery(windowKeyQuery)
                .withPartitions(Collections.singleton(keyQueryMetadata.partition())));

        if (!result.getOnlyPartitionResult().getResult().hasNext()) {
            throw new UnknownKeyException(key);
        }

        List<StateStoreRecord> partitionsResult = new ArrayList<>();
        result.getOnlyPartitionResult().getResult().forEachRemaining(kv -> {
            if (kv.value instanceof ValueAndTimestamp<?>) {
                ValueAndTimestamp<Object> valueAndTimestamp = (ValueAndTimestamp<Object>) kv.value;

                partitionsResult.add(
                    new StateStoreRecord(key, valueAndTimestamp.value(), valueAndTimestamp.timestamp())
                );
            } else {
                partitionsResult.add(new StateStoreRecord(key, kv.value));
            }
        });

        return partitionsResult;
    }
}
