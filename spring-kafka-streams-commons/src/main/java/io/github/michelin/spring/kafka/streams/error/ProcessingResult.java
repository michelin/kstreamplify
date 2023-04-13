package io.github.michelin.spring.kafka.streams.error;

import lombok.Getter;

@Getter
public class ProcessingResult<V, V2> {
    private V value;

    private ProcessingError<V2> error;

    private ProcessingResult(V value){
        this.value = value;
    }

    private ProcessingResult(ProcessingError<V2> error){
        this.error = error;
    };

    public static <V, V2> ProcessingResult<V, V2> success(V value) {
        return new ProcessingResult<>(value);
    };

    public static <V, V2> ProcessingResult<V, V2> fail(ProcessingError<V2> error) {
        return new ProcessingResult<>(error);
    }

    public boolean isValid() {
        return value != null && error == null;
    }
}
