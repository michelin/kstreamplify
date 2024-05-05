package com.michelin.kstreamplify.topology;

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
 * Topology controller test.
 */
@ExtendWith(MockitoExtension.class)
public class TopologyControllerTest {
    @InjectMocks
    private TopologyController topologyController;

    @Test
    void shouldGetTopology() {
        try (MockedStatic<TopologyService> topologyService = mockStatic(TopologyService.class)) {
            topologyService.when(() -> TopologyService.getTopology(any()))
                .thenReturn(new RestServiceResponse<>(200, "Topology"));

            ResponseEntity<String> response = topologyController.topology();

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertEquals("Topology", response.getBody());
        }
    }
}
