package com.michelin.kstreamplify.http.server;

import com.michelin.kstreamplify.http.service.RestResponse;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Http endpoint.
 */
@Getter
@AllArgsConstructor
class HttpEndpoint {
    private String path;
    private Supplier<RestResponse<?>> restService;
}
