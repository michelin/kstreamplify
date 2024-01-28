package com.michelin.kstreamplify.opentelemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;

/**
 * The OpenTelemetry configuration test class.
 */
@ExtendWith(MockitoExtension.class)
public class OpenTelemetryConfigTest {
    private final OpenTelemetryConfig openTelemetryConfig = new OpenTelemetryConfig();

    @Test
    void shouldAddTagsToMetricsWhenOpenTelemetryRegistry() {
        openTelemetryConfig.setOtelResourceAttributes("tagName=tagValue,tagName2=tagValue2");
        MeterRegistryCustomizer<MeterRegistry> customizer = openTelemetryConfig.addTagsOnMetrics();

        MeterRegistry meterRegistry = new OpenTelemetryMeterRegistry();
        customizer.customize(meterRegistry);
        meterRegistry.counter("fakeCounterMetric");

        assertEquals("fakeCounterMetric", meterRegistry.getMeters().get(0).getId().getName());
        assertEquals(Tag.of("tagName", "tagValue"), meterRegistry.getMeters().get(0).getId().getTags().get(0));
        assertEquals(Tag.of("tagName2", "tagValue2"), meterRegistry.getMeters().get(0).getId().getTags().get(1));
    }

    @Test
    void shouldNotAddTagsToMetricsIfEmpty() {
        MeterRegistryCustomizer<MeterRegistry> customizer = openTelemetryConfig.addTagsOnMetrics();

        MeterRegistry meterRegistry = new OpenTelemetryMeterRegistry();
        customizer.customize(meterRegistry);
        meterRegistry.counter("fakeCounterMetric");

        assertEquals("fakeCounterMetric", meterRegistry.getMeters().get(0).getId().getName());
        assertTrue(meterRegistry.getMeters().get(0).getId().getTags().isEmpty());
    }

    @Test
    void shouldNotAddTagsToMetricsWhenNotOpenTelemetryRegistry() {
        openTelemetryConfig.setOtelResourceAttributes("tagName=tagValue,tagName2=tagValue2");
        MeterRegistryCustomizer<MeterRegistry> customizer = openTelemetryConfig.addTagsOnMetrics();

        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        customizer.customize(meterRegistry);
        meterRegistry.counter("fakeCounterMetric");

        assertEquals("fakeCounterMetric", meterRegistry.getMeters().get(0).getId().getName());
        assertTrue(meterRegistry.getMeters().get(0).getId().getTags().isEmpty());
    }

    static class OpenTelemetryMeterRegistry extends SimpleMeterRegistry {
        // Empty class to mock OpenTelemetryMeterRegistry
    }
}
