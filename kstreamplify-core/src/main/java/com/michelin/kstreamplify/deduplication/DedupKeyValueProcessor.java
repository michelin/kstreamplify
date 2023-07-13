package com.michelin.kstreamplify.deduplication;


import com.michelin.kstreamplify.error.ProcessingResult;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.Instant;

/**
 * Transformer class for the deduplication mechanism on both keys and values of a given topic
 *
 * @param <V> The type of the value
 */
public class DedupKeyValueProcessor<V extends SpecificRecord> implements Processor<String, V, String, ProcessingResult<V, V>> {

    /**
     * Kstream context for this transformer
     */
    private ProcessorContext<String, ProcessingResult<V, V>> processorContext;
    /**
     * Window store containing all the records seen on the given window
     */
    private WindowStore<String, V> dedupWindowStore;

    /**
     * Window store name, initialized @ construction
     */
    private final String windowStoreName;
    /**
     * Retention window for the statestore. Used for fetching data
     */
    private final Duration retentionWindowDuration;

    /**
     * Constructor method
     *
     * @param windowStoreName      The window store name
     * @param retentionWindowHours The retention window duration
     */
    public DedupKeyValueProcessor(String windowStoreName, Duration retentionWindowHours) {
        this.windowStoreName = windowStoreName;
        this.retentionWindowDuration = retentionWindowHours;
    }

    @Override
    public void init(ProcessorContext<String, ProcessingResult<V, V>> context) {
        this.processorContext = context;

        dedupWindowStore = this.processorContext.getStateStore(windowStoreName);
    }

    @Override
    public void process(Record<String, V> record) {
        try {
            // Get the record timestamp
            var currentInstant = Instant.ofEpochMilli(record.timestamp());

            // Retrieve all the matching keys in the stateStore and return null if found it (signaling a duplicate)
            try (var resultIterator = dedupWindowStore.backwardFetch(record.key(), currentInstant.minus(retentionWindowDuration), currentInstant.plus(retentionWindowDuration))) {
                while (resultIterator != null && resultIterator.hasNext()) {
                    var currentKeyValue = resultIterator.next();
                    if (record.value().equals(currentKeyValue.value)) {
                        return;
                    }
                }
            }

            // First time we see this record, store entry in the window store and forward the record to the output
            dedupWindowStore.put(record.key(), record.value(), record.timestamp());
            processorContext.forward(ProcessingResult.wrapRecordSuccess(record));
        } catch (Exception e) {
            processorContext.forward(ProcessingResult.wrapRecordFailure(e, record, "Couldn't figure out what to do with the current payload: An unlikely error occured during deduplication transform"));
        }
    }
}
