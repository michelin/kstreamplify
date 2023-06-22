package io.github.michelin.kstreamplify.error;

import lombok.Getter;

/**
 * The processing result class
 * @param <V> The type of the successful record
 * @param <V2> The type of the failed record
 */
@Getter
public class ProcessingResult<V, V2> {
    /**
     * The successful record
     */
    private V value;

    /**
     * The failed record wrapped in a processing error
     */
    private ProcessingError<V2> error;

    private ProcessingResult(V value){
        this.value = value;
    }

    private ProcessingResult(ProcessingError<V2> error){
        this.error = error;
    };

    /**
     * Create a successful processing result
     * @param value The successful record
     * @return A processing result containing a successful record
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     */
    public static <V, V2> ProcessingResult<V, V2> success(V value) {
        return new ProcessingResult<>(value);
    };

    /**
     * Create a failed processing result
     * @param e The exception
     * @param kafkaRecord The failed Kafka record
     * @param contextMessage The context message
     * @return A processing result containing the failed record
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     */
    public static <V, V2> ProcessingResult<V, V2> fail(Exception e, V2 kafkaRecord, String contextMessage) {
        return new ProcessingResult<>(new ProcessingError<>(e, contextMessage, kafkaRecord));
    }

    /**
     * Create a failed processing result
     * @param e The exception
     * @param kafkaRecord The failed Kafka record
     * @return A processing result containing the failed record
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     */
    public static <V, V2> ProcessingResult<V, V2> fail(Exception e, V2 kafkaRecord) {
        return new ProcessingResult<>(new ProcessingError<>(e, kafkaRecord));
    }

    /**
     * Is the processing result valid ?
     * Is it valid either if it contains a successful value or an error
     * @return true if valid, false otherwise
     */
    public boolean isValid() {
        return value != null && error == null;
    }
}
