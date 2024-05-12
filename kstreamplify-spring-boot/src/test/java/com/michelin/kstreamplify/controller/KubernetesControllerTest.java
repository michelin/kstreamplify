package com.michelin.kstreamplify.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.model.RestResponse;
import com.michelin.kstreamplify.service.KubernetesService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@ExtendWith(MockitoExtension.class)
class KubernetesControllerTest {
    @Mock
    private KubernetesService kubernetesService;

    @InjectMocks
    private KubernetesController kubernetesController;

    @Test
    void shouldGetReadinessProbe() {
        when(kubernetesService.getReadiness())
            .thenReturn(new RestResponse<>(200, null));

        ResponseEntity<Void> response = kubernetesController.readiness();

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void shouldGetLivenessProbe() {
        when(kubernetesService.getLiveness())
            .thenReturn(new RestResponse<>(200, null));

        ResponseEntity<Void> response = kubernetesController.liveness();

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }
}
