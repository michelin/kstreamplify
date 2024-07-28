package com.michelin.kstreamplify.http.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Rest service response.
 *
 * @param <T> The body type
 */
@Getter
@Setter
@AllArgsConstructor
@Builder
public class RestResponse<T> {
    /**
     * The HTTP status.
     */
    private int status;

    /**
     * The request body.
     */
    private T body;
}
