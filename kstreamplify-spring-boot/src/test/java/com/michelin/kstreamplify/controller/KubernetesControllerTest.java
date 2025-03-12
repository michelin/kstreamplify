/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

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
        when(kubernetesService.getReadiness()).thenReturn(200);

        ResponseEntity<Void> response = kubernetesController.readiness();

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void shouldGetLivenessProbe() {
        when(kubernetesService.getLiveness()).thenReturn(200);

        ResponseEntity<Void> response = kubernetesController.liveness();

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }
}
