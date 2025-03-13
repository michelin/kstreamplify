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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/** The OpenTelemetry configuration class. */
@Slf4j
@Getter
@Setter
@Configuration
public class OpenTelemetryConfig {
    /** The OpenTelemetry resource attributes. */
    @Value("${otel.resource.attributes:#{null}}")
    private String otelResourceAttributes;

    /**
     * Register tags in Open Telemetry meter registry. It enables to add custom tags given in the property
     * otel.resource.attributes to metrics.
     *
     * @return A meter registry customizer
     */
    @Bean
    @ConditionalOnProperty(value = "otel.resource.attributes")
    public MeterRegistryCustomizer<MeterRegistry> addTagsOnMetrics() {
        List<Tag> tags = StringUtils.hasText(otelResourceAttributes)
                ? Arrays.stream(otelResourceAttributes.split(","))
                        .map(resourceAttribute -> Tag.of(
                                resourceAttribute.split("=")[0],
                                resourceAttribute.split("=")[1]))
                        .toList()
                : Collections.emptyList();

        return registry -> {
            // Only add tags and Kafka metrics to Open Telemetry meter registry whose Java agent reads from it
            if (registry.getClass().getName().contains("OpenTelemetryMeterRegistry")) {
                log.info(
                        "Adding tags {} to registry {}",
                        tags.stream()
                                .map(tag -> tag.getKey() + "=" + tag.getValue())
                                .toList(),
                        registry.getClass().getName());
                registry.config().commonTags(tags);
            }
        };
    }
}
