package com.michelin.kstreamplify.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import com.michelin.kstreamplify.model.RestServiceResponse;
import com.michelin.kstreamplify.services.ProbeService;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class SpringProbeControllerTest {
    private final SpringProbeController controller = new SpringProbeController();

    @Test
    void shouldGetReadinessProbe() {
        try (MockedStatic<ProbeService> probeService = mockStatic(ProbeService.class)) {
            probeService.when(() -> ProbeService.readinessProbe(any()))
                .thenReturn(new RestServiceResponse<>(200, "Ready"));

            ResponseEntity<String> response = controller.readinessProbe();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Ready", response.getBody());
        }
    }

    @Test
    void shouldGetLivenessProbe() {
        try (MockedStatic<ProbeService> probeService = mockStatic(ProbeService.class)) {
            probeService.when(() -> ProbeService.livenessProbe(any()))
                .thenReturn(new RestServiceResponse<>(200, "Alive"));

            ResponseEntity<String> response = controller.livenessProbe();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Alive", response.getBody());
        }
    }

    @Test
    void shouldGetTopology() {
        try (MockedStatic<ProbeService> probeService = mockStatic(ProbeService.class)) {
            probeService.when(() -> ProbeService.exposeTopology(any()))
                .thenReturn(new RestServiceResponse<>(200, "Topology"));

            ResponseEntity<String> response = controller.exposeTopology();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Topology", response.getBody());
        }
    }
}
