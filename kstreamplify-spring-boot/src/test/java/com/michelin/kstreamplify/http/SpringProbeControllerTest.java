package com.michelin.kstreamplify.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import com.michelin.kstreamplify.model.RestServiceResponse;
import com.michelin.kstreamplify.kubernetes.KubernetesService;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class SpringProbeControllerTest {
    private final SpringProbeController controller = new SpringProbeController();

    @Test
    void shouldGetReadinessProbe() {
        try (MockedStatic<KubernetesService> probeService = mockStatic(KubernetesService.class)) {
            probeService.when(() -> KubernetesService.getReadiness(any()))
                .thenReturn(new RestServiceResponse<>(200, "Ready"));

            ResponseEntity<String> response = controller.readinessProbe();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Ready", response.getBody());
        }
    }

    @Test
    void shouldGetLivenessProbe() {
        try (MockedStatic<KubernetesService> probeService = mockStatic(KubernetesService.class)) {
            probeService.when(() -> KubernetesService.getLiveness(any()))
                .thenReturn(new RestServiceResponse<>(200, "Alive"));

            ResponseEntity<String> response = controller.livenessProbe();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Alive", response.getBody());
        }
    }

    @Test
    void shouldGetTopology() {
        try (MockedStatic<KubernetesService> probeService = mockStatic(KubernetesService.class)) {
            probeService.when(() -> KubernetesService.exposeTopology(any()))
                .thenReturn(new RestServiceResponse<>(200, "Topology"));

            ResponseEntity<String> response = controller.exposeTopology();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Topology", response.getBody());
        }
    }
}
