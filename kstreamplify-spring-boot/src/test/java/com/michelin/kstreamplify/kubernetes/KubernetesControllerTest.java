package com.michelin.kstreamplify.kubernetes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import com.michelin.kstreamplify.model.RestServiceResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Kubernetes controller test.
 */
@ExtendWith(MockitoExtension.class)
public class KubernetesControllerTest {
    @InjectMocks
    private KubernetesController kubernetesController;

    @Test
    void shouldGetReadinessProbe() {
        try (MockedStatic<KubernetesService> kubernetesService = mockStatic(KubernetesService.class)) {
            kubernetesService.when(() -> KubernetesService.getReadiness(any()))
                .thenReturn(new RestServiceResponse<>(200, (Void) null));

            ResponseEntity<Void> response = kubernetesController.readiness();

            assertEquals(HttpStatus.OK, response.getStatusCode());
        }
    }

    @Test
    void shouldGetLivenessProbe() {
        try (MockedStatic<KubernetesService> kubernetesService = mockStatic(KubernetesService.class)) {
            kubernetesService.when(() -> KubernetesService.getLiveness(any()))
                .thenReturn(new RestServiceResponse<>(200, (Void) null));

            ResponseEntity<Void> response = kubernetesController.liveness();

            assertEquals(HttpStatus.OK, response.getStatusCode());
        }
    }
}
