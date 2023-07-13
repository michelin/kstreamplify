package com.michelin.kstreamplify.deduplication;


import com.michelin.kstreamplify.error.ProcessingResult;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Duration;
import java.util.function.Function;

/**
 * Transformer class for the deduplication mechanism on keys of a given topic
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class DedupWithPredicateProcessor<K, V extends SpecificRecord> implements Processor<K, V, K, ProcessingResult<V, V>> {

    /**
     * Kstream context for this transformer
     */
    private ProcessorContext<K, ProcessingResult<V, V>> processorContext;
    /**
     * Window store containing all the records seen on the given window
     */
    private TimestampedKeyValueStore<String, V> dedupTimestampedStore;

    /**
     * Window store name, initialized @ construction
     */
    private final String dedupStoreName;

    /**
     * Retention window for the statestore. Used for fetching data
     */
    private final Duration retentionWindowDuration;

    private Function<V, String> dedupFunction;

    public DedupWithPredicateProcessor(String dedupStoreName, Duration retentionWindowHours, Function<V, String> dedupFunction) {
        this.dedupStoreName = dedupStoreName;
        this.retentionWindowDuration = retentionWindowHours;
        this.dedupFunction = dedupFunction;
    }

    @Override
    public void init(ProcessorContext<K, ProcessingResult<V, V>> context) {
        this.processorContext = context;

        dedupTimestampedStore = this.processorContext.getStateStore(dedupStoreName);

        processorContext.schedule(Duration.ofHours(1), PunctuationType.WALL_CLOCK_TIME, (currentTimestamp) -> {
            try (var iterator = dedupTimestampedStore.all()) {
                while (iterator.hasNext()) {
                    var currentRecord = iterator.next();
                    if (currentRecord.value.timestamp() + retentionWindowDuration.toMillis() < currentTimestamp) {
                        dedupTimestampedStore.delete(currentRecord.key);
                    }
                }
            }
        });
    }

    @Override
    public void process(Record<K, V> record) {
        try {

            String identifier = dedupFunction.apply(record.value());
            // Retrieve the matching identifier in the statestore and return null if found it (signaling a duplicate)
            if (dedupTimestampedStore.get(identifier) == null) {
                // First time we see this record, store entry in the windowstore and forward the record to the output
                dedupTimestampedStore.put(identifier, ValueAndTimestamp.make(record.value(), record.timestamp()));
                processorContext.forward(ProcessingResult.wrapRecordSuccess(record));
            }
        } catch (Exception e) {
            processorContext.forward(ProcessingResult.wrapRecordFailure(e, record, "Couldn't figure out what to do with the current payload: An unlikely error occured during deduplication transform"));
        }
    }
}
