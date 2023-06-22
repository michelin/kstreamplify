package io.github.michelin.kstreamplify.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@Builder
public class RestServiceResponse<T> {
    
    private int status;
    
    private T body;
}
