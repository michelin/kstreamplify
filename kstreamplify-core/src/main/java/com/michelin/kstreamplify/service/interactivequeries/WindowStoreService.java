package com.michelin.kstreamplify.service.interactivequeries;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.store.StateStoreRecord;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.UnknownStateStoreException;

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
}
