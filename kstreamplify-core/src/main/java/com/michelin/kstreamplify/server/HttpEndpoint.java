package com.michelin.kstreamplify.server;

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
