package com.michelin.kstreamplify.opentelemetry;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * The OpenTelemetry configuration class.
 */
@Slf4j
@Getter
@Setter
@Configuration
public class OpenTelemetryConfig {
    /**
     * The OpenTelemetry resource attributes.
     */
    @Value("${otel.resource.attributes}")
    private String otelResourceAttributes;

    /**
     * Register tags in Open Telemetry meter registry.
     * It enables to add custom tags given in the property otel.resource.attributes
     * to metrics.
     *
     * @return A meter registry customizer
     */
    @Bean
    @ConditionalOnProperty(value = "otel.resource.attributes")
    public MeterRegistryCustomizer<MeterRegistry> addTagsOnMetrics() {
        List<Tag> tags = StringUtils.hasText(otelResourceAttributes)
            ? Arrays.stream(otelResourceAttributes.split(","))
                .map(resourceAttribute -> Tag.of(resourceAttribute.split("=")[0], resourceAttribute.split("=")[1]))
                .toList() : Collections.emptyList();

        return registry -> {
            // Only add tags and Kafka metrics to Open Telemetry meter registry whose Java agent reads from it
            if (registry.getClass().getName().contains("OpenTelemetryMeterRegistry")) {
                log.info("Adding tags {} to registry {}",
                    tags.stream().map(tag -> tag.getKey() + "=" + tag.getValue()).toList(),
                    registry.getClass().getName());
                registry.config().commonTags(tags);
            }
        };
    }
}
