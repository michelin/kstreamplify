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
package com.michelin.kstreamplify.property;

import static com.michelin.kstreamplify.property.KstreamplifyConfig.DLQ_DESERIALIZATION_HANDLER_FORWARD_REST_CLIENT_EXCEPTION;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class PropertiesUtilsTest {

    @Test
    void shouldLoadProperties() {
        Properties properties = PropertiesUtils.loadProperties();

        assertTrue(properties.containsKey("server.port"));
        assertEquals(8080, properties.get("server.port"));

        assertTrue(properties.containsKey("kafka.properties." + APPLICATION_ID_CONFIG));
        assertEquals("appId", properties.get("kafka.properties." + APPLICATION_ID_CONFIG));

        assertTrue(properties.containsKey(
                "kafka.properties." + DLQ_DESERIALIZATION_HANDLER_FORWARD_REST_CLIENT_EXCEPTION));
        assertEquals(
                true, properties.get("kafka.properties." + DLQ_DESERIALIZATION_HANDLER_FORWARD_REST_CLIENT_EXCEPTION));
    }

    @Test
    void shouldLoadKafkaProperties() {
        Properties properties = PropertiesUtils.loadKafkaProperties(PropertiesUtils.loadProperties());

        assertTrue(properties.containsKey(APPLICATION_ID_CONFIG));
        assertTrue(properties.containsValue("appId"));
    }

    @Test
    void shouldExtractPropertiesByPrefix() {
        Properties props = new Properties();
        props.put("dlq.feature1", "true");
        props.put("dlq.feature2", "false");
        props.put("other.feature", "ignored");

        Properties extracted = PropertiesUtils.extractPropertiesByPrefix(props, "dlq.");

        assertEquals(2, extracted.size());
        assertEquals("true", extracted.getProperty("dlq.feature1"));
        assertEquals("false", extracted.getProperty("dlq.feature2"));
        assertNull(extracted.getProperty("other.feature"));
    }

    @Test
    void shouldReturnTrueWhenFeatureEnabled() {
        Properties props = new Properties();
        props.put("my.feature", "true");

        assertTrue(PropertiesUtils.isFeatureEnabled(props, "my.feature", false));
    }

    @Test
    void shouldReturnFalseWhenFeatureDisabled() {
        Properties props = new Properties();
        props.put("my.feature", "false");

        assertFalse(PropertiesUtils.isFeatureEnabled(props, "my.feature", true));
    }

    @Test
    void shouldReturnDefaultWhenFeatureMissing() {
        Properties props = new Properties();

        assertTrue(PropertiesUtils.isFeatureEnabled(props, "missing.feature", true));
        assertFalse(PropertiesUtils.isFeatureEnabled(props, "missing.feature", false));
    }
}
