package com.michelin.kstreamplify.deduplication;


import com.michelin.kstreamplify.error.ProcessingResult;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;


/**
 * Transformer class for the deduplication mechanism on predicate of a given topic.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class DedupWithPredicateProcessor<K, V extends SpecificRecord>
    implements Processor<K, V, K, ProcessingResult<V, V>> {

    /**
     * Kstream context for this transformer.
     */
    private ProcessorContext<K, ProcessingResult<V, V>> processorContext;

    /**
     * Window store containing all the records seen on the given window.
     */
    private WindowStore<String, V> dedupWindowStore;

    /**
     * Window store name, initialized @ construction.
     */
    private final String windowStoreName;

    /**
     * Retention window for the statestore. Used for fetching data.
     */
    private final Duration retentionWindowDuration;

    /**
     * Deduplication key extractor.
     */
    private final Function<V, String> deduplicationKeyExtractor;

    /**
     * Constructor.
     *
     * @param windowStoreName           Name of the deduplication state store
     * @param retentionWindowDuration   Retention window duration
     * @param deduplicationKeyExtractor Deduplication function
     */
    public DedupWithPredicateProcessor(String windowStoreName, Duration retentionWindowDuration,
                                       Function<V, String> deduplicationKeyExtractor) {
        this.windowStoreName = windowStoreName;
        this.retentionWindowDuration = retentionWindowDuration;
        this.deduplicationKeyExtractor = deduplicationKeyExtractor;
    }

    @Override
    public void init(ProcessorContext<K, ProcessingResult<V, V>> context) {
        this.processorContext = context;
        dedupWindowStore = this.processorContext.getStateStore(windowStoreName);
    }

    @Override
    public void process(Record<K, V> message) {
        try {
            // Get the record timestamp
            var currentInstant = Instant.ofEpochMilli(message.timestamp());
            String identifier = deduplicationKeyExtractor.apply(message.value());

            // Retrieve all the matching keys in the stateStore and return null if found it (signaling a duplicate)
            try (var resultIterator = dedupWindowStore.backwardFetch(identifier,
                currentInstant.minus(retentionWindowDuration),
                currentInstant.plus(retentionWindowDuration))) {
                while (resultIterator != null && resultIterator.hasNext()) {
                    var currentKeyValue = resultIterator.next();
                    if (identifier.equals(deduplicationKeyExtractor.apply(currentKeyValue.value))) {
                        return;
                    }
                }
            }

            // First time we see this record, store entry in the window store and forward the record to the output
            dedupWindowStore.put(identifier, message.value(), message.timestamp());
            processorContext.forward(ProcessingResult.wrapRecordSuccess(message));

        } catch (Exception e) {
            processorContext.forward(ProcessingResult.wrapRecordFailure(e, message,
                "Could not figure out what to do with the current payload: "
                    + "An unlikely error occurred during deduplication transform"));
        }
    }
}
