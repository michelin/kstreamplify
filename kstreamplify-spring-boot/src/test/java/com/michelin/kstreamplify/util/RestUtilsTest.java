package com.michelin.kstreamplify.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.michelin.kstreamplify.model.RestServiceResponse;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

/**
 * REST utils test.
 */
public class RestUtilsTest {

    @Test
    void shouldConvertToResponseEntity() {
        RestServiceResponse<String> serviceResponse = new RestServiceResponse<>(200, "OK");
        ResponseEntity<String> responseEntity = RestUtils.toResponseEntity(serviceResponse);
        assertEquals(200, responseEntity.getStatusCode().value());
        assertEquals("OK", responseEntity.getBody());
    }
}
