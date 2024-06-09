package com.michelin.kstreamplify.server;

import lombok.Builder;

/**
 * Rest response.
 *
 * @param status The status
 * @param body The body
 * @param <T> The body type
 */
@Builder
public record RestResponse<T>(int status, T body) { }
