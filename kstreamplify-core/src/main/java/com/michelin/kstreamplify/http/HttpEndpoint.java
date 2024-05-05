package com.michelin.kstreamplify.http;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.model.RestServiceResponse;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Http endpoint.
 */
@Getter
@AllArgsConstructor
public class HttpEndpoint {
    private String path;
    private Function<KafkaStreamsInitializer, RestServiceResponse<?>> restService;
}
