package com.michelin.kstreamplify.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.converter.AvroToJsonConverter;
import com.michelin.kstreamplify.initializer.SpringKafkaStreamsInitializer;
import com.michelin.kstreamplify.model.RestServiceResponse;
import com.michelin.kstreamplify.services.ProbeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;

public class SpringProbeControllerTest {

    private SpringProbeController controller;
    private SpringKafkaStreamsInitializer kafkaStreamsInitializer;

    @BeforeEach
    public void setUp() {
        kafkaStreamsInitializer = mock(SpringKafkaStreamsInitializer.class);
        controller = new SpringProbeController();
        setPrivateField(controller, "kafkaStreamsInitializer", kafkaStreamsInitializer);
    }

    @Test
    public void testReadinessProbe() {
        try (MockedStatic<ProbeService> probeService = mockStatic(ProbeService.class)) {
            probeService.when(() -> ProbeService.readinessProbe(any())).thenReturn(new RestServiceResponse<>(200, "Ready"));

            ResponseEntity<String> response = controller.readinessProbe();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Ready", response.getBody());
        }
    }

    @Test
    public void testLivenessProbe() {
        try (MockedStatic<ProbeService> probeService = mockStatic(ProbeService.class)) {
            probeService.when(() -> ProbeService.livenessProbe(any())).thenReturn(new RestServiceResponse<>(200, "Alive"));

            ResponseEntity<String> response = controller.livenessProbe();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Alive", response.getBody());
        }
    }

    @Test
    public void testExposeTopology() {
        try (MockedStatic<ProbeService> probeService = mockStatic(ProbeService.class)) {
            probeService.when(() -> ProbeService.exposeTopology(any())).thenReturn(new RestServiceResponse<>(200, "Topology"));

            ResponseEntity<String> response = controller.exposeTopology();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Topology", response.getBody());
        }
    }

    private void setPrivateField(Object object, String fieldName, Object value) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
