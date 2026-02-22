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
import static com.michelin.kstreamplify.property.PropertiesUtils.KAFKA_PROPERTIES_PREFIX;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class PropertiesUtilsTest {

    @Test
    void shouldLoadProperties() {
        Properties properties = PropertiesUtils.loadProperties();

        assertTrue(properties.containsKey("server.port"));
        assertEquals(8080, properties.get("server.port"));

        assertTrue(properties.containsKey(KAFKA_PROPERTIES_PREFIX + APPLICATION_ID_CONFIG));
        assertEquals("appId", properties.get(KAFKA_PROPERTIES_PREFIX + APPLICATION_ID_CONFIG));

        assertTrue(properties.containsKey(KAFKA_PROPERTIES_PREFIX + AUTO_OFFSET_RESET_CONFIG));
        assertEquals("earliest", properties.get(KAFKA_PROPERTIES_PREFIX + AUTO_OFFSET_RESET_CONFIG));

        assertTrue(properties.containsKey(KAFKA_PROPERTIES_PREFIX + DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertEquals(
                "org.apache.kafka.common.serialization.Serdes$StringSerde",
                properties.get(KAFKA_PROPERTIES_PREFIX + DEFAULT_KEY_SERDE_CLASS_CONFIG));

        assertTrue(properties.containsKey(KAFKA_PROPERTIES_PREFIX + DEFAULT_VALUE_SERDE_CLASS_CONFIG));
        assertEquals(
                "org.apache.kafka.common.serialization.Serdes$StringSerde",
                properties.get(KAFKA_PROPERTIES_PREFIX + DEFAULT_VALUE_SERDE_CLASS_CONFIG));

        assertTrue(properties.containsKey(KAFKA_PROPERTIES_PREFIX + STATE_DIR_CONFIG));
        assertEquals(
                "/tmp/kstreamplify/kstreamplify-core-test/initializer",
                properties.get(KAFKA_PROPERTIES_PREFIX + STATE_DIR_CONFIG));

        assertTrue(properties.containsKey(
                KAFKA_PROPERTIES_PREFIX + DLQ_DESERIALIZATION_HANDLER_FORWARD_REST_CLIENT_EXCEPTION));
        assertEquals(
                true,
                properties.get(KAFKA_PROPERTIES_PREFIX + DLQ_DESERIALIZATION_HANDLER_FORWARD_REST_CLIENT_EXCEPTION));
    }

    @Test
    void shouldExtractSubProperties() {
        Properties properties =
                PropertiesUtils.extractSubProperties(PropertiesUtils.loadProperties(), KAFKA_PROPERTIES_PREFIX, true);

        assertTrue(properties.containsKey(APPLICATION_ID_CONFIG));
        assertEquals("appId", properties.get(APPLICATION_ID_CONFIG));

        assertTrue(properties.containsKey(AUTO_OFFSET_RESET_CONFIG));
        assertEquals("earliest", properties.get(AUTO_OFFSET_RESET_CONFIG));

        assertTrue(properties.containsKey(DEFAULT_KEY_SERDE_CLASS_CONFIG));
        assertEquals(
                "org.apache.kafka.common.serialization.Serdes$StringSerde",
                properties.get(DEFAULT_KEY_SERDE_CLASS_CONFIG));

        assertTrue(properties.containsKey(DEFAULT_VALUE_SERDE_CLASS_CONFIG));
        assertEquals(
                "org.apache.kafka.common.serialization.Serdes$StringSerde",
                properties.get(DEFAULT_VALUE_SERDE_CLASS_CONFIG));

        assertTrue(properties.containsKey(STATE_DIR_CONFIG));
        assertEquals("/tmp/kstreamplify/kstreamplify-core-test/initializer", properties.get(STATE_DIR_CONFIG));

        assertFalse(properties.containsKey(DLQ_DESERIALIZATION_HANDLER_FORWARD_REST_CLIENT_EXCEPTION));
        assertFalse(properties.containsKey("server.port"));
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
