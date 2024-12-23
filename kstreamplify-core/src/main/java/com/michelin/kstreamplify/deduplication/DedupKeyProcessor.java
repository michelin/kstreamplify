package com.michelin.kstreamplify.deduplication;

import com.michelin.kstreamplify.error.ProcessingResult;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.Instant;

/**
 * Transformer class for the deduplication mechanism on keys of a given topic.
 *
 * @param <V> The type of the value
 */
public class DedupKeyProcessor<V extends SpecificRecord>
        implements Processor<String, V, String, ProcessingResult<V, V>> {

    /**
     * Kstream context for this transformer.
     */
    private ProcessorContext<String, ProcessingResult<V, V>> processorContext;

    /**
     * Window store containing all the records seen on the given window.
     */
    private WindowStore<String, String> dedupWindowStore;

    /**
     * TimestampKeyValue store containing all the records to migrate into Window store.
     */
    private TimestampedKeyValueStore<String, String> timestampKeyValueStore;

    /**
     * Window store name, initialized @ construction.
     */
    private final String windowStoreName;

    /**
     * TimestampKeyValue store name, initialized @ construction.
     */
    private final String timestampKeyValueStoreName;


    /**
     * Retention window for the state store. Used for fetching data.
     */
    private final Duration retentionWindowDuration;

    /**
     * Constructor.
     *
     * @param windowStoreName         The name of the constructor
     * @param retentionWindowDuration The retentionWindow Duration
     */
    public DedupKeyProcessor(String windowStoreName, Duration retentionWindowDuration, String timestampKeyValueStoreName) {
        this.windowStoreName = windowStoreName;
        this.timestampKeyValueStoreName = timestampKeyValueStoreName;
        this.retentionWindowDuration = retentionWindowDuration;
    }

    @Override
    public void init(ProcessorContext<String, ProcessingResult<V, V>> context) {
        processorContext = context;
        dedupWindowStore = this.processorContext.getStateStore(windowStoreName);
        if (!StringUtils.isEmpty(timestampKeyValueStoreName)) {
            timestampKeyValueStore = this.processorContext.getStateStore(timestampKeyValueStoreName);
            migrateDataToWindowStore();
        }
    }

    private void migrateDataToWindowStore() {
        KeyValueIterator<String, ValueAndTimestamp<String>> iterator = timestampKeyValueStore.all();

        while (iterator.hasNext()) {
            KeyValue<String, ValueAndTimestamp<String>> entry = iterator.next();
            String key = entry.key;
            String value = entry.value.value();


            // Insérer les données dans le WindowStore
            dedupWindowStore.put(key, value, entry.value.timestamp());
            timestampKeyValueStore.delete(key);
        }

        iterator.close();
    }

    @Override
    public void process(Record<String, V> message) {
        try {
            // Get the record timestamp
            var currentInstant = Instant.ofEpochMilli(message.timestamp());

            // Retrieve all the matching keys in the stateStore and return null if found it (signaling a duplicate)
            try (var resultIterator = dedupWindowStore.backwardFetch(message.key(),
                    currentInstant.minus(retentionWindowDuration),
                    currentInstant.plus(retentionWindowDuration))) {
                while (resultIterator != null && resultIterator.hasNext()) {
                    var currentKeyValue = resultIterator.next();
                    if (message.key().equals(currentKeyValue.value)) {
                        return;
                    }
                }
            }

            // First time we see this record, store entry in the window store and forward the record to the output
            dedupWindowStore.put(message.key(), message.key(), message.timestamp());
            processorContext.forward(ProcessingResult.wrapRecordSuccess(message));
        } catch (Exception e) {
            processorContext.forward(ProcessingResult.wrapRecordFailure(e, message,
                    "Could not figure out what to do with the current payload: "
                            + "An unlikely error occurred during deduplication transform"));
        }
    }
}
