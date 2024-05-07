package com.michelin.kstreamplify.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.kstreamplify.http.service.RestResponse;
import com.michelin.kstreamplify.http.service.TopologyService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Topology controller test.
 */
@ExtendWith(MockitoExtension.class)
public class TopologyControllerTest {
    @Mock
    private TopologyService topologyService;

    @InjectMocks
    private TopologyController topologyController;

    @Test
    void shouldGetTopology() {
        when(topologyService.getTopology())
            .thenReturn(new RestResponse<>(200, "Topology"));

        ResponseEntity<String> response = topologyController.topology();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Topology", response.getBody());
    }
}
