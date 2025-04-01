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
package com.michelin.kstreamplify.opentelemetry;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;

@ExtendWith(MockitoExtension.class)
class OpenTelemetryConfigTest {
    private final OpenTelemetryConfig openTelemetryConfig = new OpenTelemetryConfig();

    @Test
    void shouldNotAddTagsToMetricsWhenNull() {
        openTelemetryConfig.setOtelResourceAttributes(null);
        MeterRegistryCustomizer<MeterRegistry> customizer = openTelemetryConfig.addTagsOnMetrics();

        MeterRegistry meterRegistry = new OpenTelemetryMeterRegistry();
        customizer.customize(meterRegistry);
        meterRegistry.counter("fakeCounterMetric");

        assertEquals(
                "fakeCounterMetric", meterRegistry.getMeters().get(0).getId().getName());
        assertTrue(meterRegistry.getMeters().get(0).getId().getTags().isEmpty());
    }

    @Test
    void shouldNotAddTagsToMetricsWhenEmpty() {
        openTelemetryConfig.setOtelResourceAttributes(EMPTY);
        MeterRegistryCustomizer<MeterRegistry> customizer = openTelemetryConfig.addTagsOnMetrics();

        MeterRegistry meterRegistry = new OpenTelemetryMeterRegistry();
        customizer.customize(meterRegistry);
        meterRegistry.counter("fakeCounterMetric");

        assertEquals(
                "fakeCounterMetric", meterRegistry.getMeters().get(0).getId().getName());
        assertTrue(meterRegistry.getMeters().get(0).getId().getTags().isEmpty());
    }

    @Test
    void shouldAddTagsToMetricsWhenOpenTelemetryRegistry() {
        openTelemetryConfig.setOtelResourceAttributes("tagName=tagValue,tagName2=tagValue2");
        MeterRegistryCustomizer<MeterRegistry> customizer = openTelemetryConfig.addTagsOnMetrics();

        MeterRegistry meterRegistry = new OpenTelemetryMeterRegistry();
        customizer.customize(meterRegistry);
        meterRegistry.counter("fakeCounterMetric");

        assertEquals(
                "fakeCounterMetric", meterRegistry.getMeters().get(0).getId().getName());
        assertEquals(
                Tag.of("tagName", "tagValue"),
                meterRegistry.getMeters().get(0).getId().getTags().get(0));
        assertEquals(
                Tag.of("tagName2", "tagValue2"),
                meterRegistry.getMeters().get(0).getId().getTags().get(1));
    }

    @Test
    void shouldNotAddTagsToMetricsIfEmpty() {
        MeterRegistryCustomizer<MeterRegistry> customizer = openTelemetryConfig.addTagsOnMetrics();

        MeterRegistry meterRegistry = new OpenTelemetryMeterRegistry();
        customizer.customize(meterRegistry);
        meterRegistry.counter("fakeCounterMetric");

        assertEquals(
                "fakeCounterMetric", meterRegistry.getMeters().get(0).getId().getName());
        assertTrue(meterRegistry.getMeters().get(0).getId().getTags().isEmpty());
    }

    @Test
    void shouldNotAddTagsToMetricsWhenNotOpenTelemetryRegistry() {
        openTelemetryConfig.setOtelResourceAttributes("tagName=tagValue,tagName2=tagValue2");
        MeterRegistryCustomizer<MeterRegistry> customizer = openTelemetryConfig.addTagsOnMetrics();

        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        customizer.customize(meterRegistry);
        meterRegistry.counter("fakeCounterMetric");

        assertEquals(
                "fakeCounterMetric", meterRegistry.getMeters().get(0).getId().getName());
        assertTrue(meterRegistry.getMeters().get(0).getId().getTags().isEmpty());
    }

    static class OpenTelemetryMeterRegistry extends SimpleMeterRegistry {
        // Empty class to mock OpenTelemetryMeterRegistry
    }
}
