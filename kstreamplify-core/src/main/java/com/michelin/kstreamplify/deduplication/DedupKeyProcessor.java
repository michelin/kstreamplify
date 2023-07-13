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

/**
 * Transformer class for the deduplication mechanism on keys of a given topic
 *
 * @param <V> The type of the value
 */
public class DedupKeyProcessor<V extends SpecificRecord> implements Processor<String, V, String, ProcessingResult<V, V>> {

    /**
     * Kstream context for this transformer
     */
    private ProcessorContext<String, ProcessingResult<V, V>> processorContext;
    /**
     * Window store containing all the records seen on the given window
     */
    private TimestampedKeyValueStore<String, String> dedupTimestampedStore;

    /**
     * Window store name, initialized @ construction
     */
    private final String dedupStoreName;

    /**
     * Retention window for the statestore. Used for fetching data
     */
    private final Duration retentionWindowDuration;

    /**
     * Constructor
     *
     * @param dedupStoreName          The name of the constructor
     * @param retentionWindowDuration The retentionWindow Duration
     */
    public DedupKeyProcessor(String dedupStoreName, Duration retentionWindowDuration) {
        this.dedupStoreName = dedupStoreName;
        this.retentionWindowDuration = retentionWindowDuration;
    }

    @Override
    public void init(ProcessorContext<String, ProcessingResult<V, V>> context) {
        processorContext = context;

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
    public void process(Record<String, V> record) {
        String key = record.key();
        try {
            // Retrieve the matching key in the statestore and return null if found (signaling a duplicate)
            if (dedupTimestampedStore.get(key) == null) {

                // First time we see this record, store entry in the windowstore and forward the record to the output
                dedupTimestampedStore.put(key, ValueAndTimestamp.make(key, processorContext.currentStreamTimeMs()));

                processorContext.forward(ProcessingResult.wrapRecordSuccess(record));
            }
        } catch (Exception e) {
            processorContext.forward(ProcessingResult.wrapRecordFailure(e, record, "Couldn't figure out what to do with the current payload: An unlikely error occurred during deduplication transform"));
        }
    }

}
