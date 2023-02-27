package com.michelin.kafka.streams.starter.test.error;

/**
 * @param <V> type of returned value in case of processing success
 * @param <EV> type of initial value in case of processing exception
 *            
 * Hold the result of a V value processing
 */
public class ProcessingResult<V,EV> {

    private ProcessingException<EV> exception;
    private V value;
    
    public static <V,EV>ProcessingResult<V,EV> success(V value) {
        return new ProcessingResult<V,EV>(value);
    };

    public static <V,EV>ProcessingResult<V,EV> fail(ProcessingException<EV> processException) {
        return new ProcessingResult<>(processException);
    }

    private ProcessingResult(ProcessingException<EV> exception){
        this.exception = exception;
    };

    private ProcessingResult(V value){
        this.value = value;
    }


    public V getValue() {
        return value;
    }
    public ProcessingException<EV> getException() {
        return exception;
    }


    /**
     * @return true if processing result is ok
     */
    public boolean isValid() {
        return value != null & exception == null;
    }
}
