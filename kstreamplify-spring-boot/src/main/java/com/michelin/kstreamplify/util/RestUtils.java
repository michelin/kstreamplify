package com.michelin.kstreamplify.util;

import com.michelin.kstreamplify.model.RestServiceResponse;
import org.springframework.http.ResponseEntity;

/**
 * REST utils.
 */
public class RestUtils {
    /**
     * Convert the probe service response into an HTTP response entity.
     *
     * @param serviceResponse The probe service response
     * @param <T>             The response body type
     * @return An HTTP response
     */
    public static <T> ResponseEntity<T> toResponseEntity(RestServiceResponse<T> serviceResponse) {
        return ResponseEntity.status(serviceResponse.getStatus()).body(serviceResponse.getBody());
    }
}
