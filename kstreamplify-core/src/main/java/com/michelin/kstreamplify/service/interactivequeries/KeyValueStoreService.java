package com.michelin.kstreamplify.service.interactivequeries;

import com.michelin.kstreamplify.exception.UnknownKeyException;
import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Key-value store service.
 */
@Slf4j
public class KeyValueStoreService extends InteractiveQueriesService {

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     */
    public KeyValueStoreService(KafkaStreamsInitializer kafkaStreamsInitializer) {
        super(kafkaStreamsInitializer);
    }

    /**
     * Constructor.
     *
     * @param kafkaStreamsInitializer The Kafka Streams initializer
     * @param httpClient The HTTP client
     */
    @SuppressWarnings("unused")
    public KeyValueStoreService(KafkaStreamsInitializer kafkaStreamsInitializer, HttpClient httpClient) {
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
                    getAllOnOtherHost(metadata.hostInfo(), "store/key-value/local/" + store)
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

            return getByKeyOnOtherHost(host, "store/key-value/" + store + "/" + key);
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
        RangeQuery<String, Object> rangeQuery = RangeQuery.withNoBounds();
        StateQueryResult<KeyValueIterator<String, Object>> result = kafkaStreamsInitializer
            .getKafkaStreams()
            .query(StateQueryRequest
                .inStore(store)
                .withQuery(rangeQuery));

        List<StateStoreRecord> partitionsResult = new ArrayList<>();
        result.getPartitionResults().forEach((key, queryResult) ->
            queryResult.getResult().forEachRemaining(kv -> {
                if (kv.value instanceof ValueAndTimestamp<?>) {
                    ValueAndTimestamp<Object> valueAndTimestamp = (ValueAndTimestamp<Object>) kv.value;

                    partitionsResult.add(
                        new StateStoreRecord(
                            kv.key,
                            valueAndTimestamp.value(),
                            valueAndTimestamp.timestamp()
                        )
                    );
                } else {
                    partitionsResult.add(new StateStoreRecord(kv.key, kv.value));
                }
            }));

        return new ArrayList<>(partitionsResult);
    }

    @SuppressWarnings("unchecked")
    private StateStoreRecord executeKeyQuery(KeyQueryMetadata keyQueryMetadata, String store, String key) {
        KeyQuery<String, Object> keyQuery = KeyQuery.withKey(key);
        StateQueryResult<Object> result = kafkaStreamsInitializer
            .getKafkaStreams()
            .query(StateQueryRequest
                .inStore(store)
                .withQuery(keyQuery)
                .withPartitions(Collections.singleton(keyQueryMetadata.partition())));

        if (result.getOnlyPartitionResult() == null) {
            throw new UnknownKeyException(key);
        }

        if (result.getOnlyPartitionResult().getResult() instanceof ValueAndTimestamp<?>) {
            ValueAndTimestamp<Object> valueAndTimestamp = (ValueAndTimestamp<Object>) result.getOnlyPartitionResult()
                .getResult();

            return new StateStoreRecord(key, valueAndTimestamp.value(), valueAndTimestamp.timestamp());
        }

        return new StateStoreRecord(key, result.getOnlyPartitionResult().getResult());
    }
}
