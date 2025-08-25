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
package com.michelin.kstreamplify.context;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsExecutionContextTest {

    @BeforeEach
    void setUp() {
        KafkaStreamsExecutionContext.setProperties(null);
        KafkaStreamsExecutionContext.setDlqProperties(new Properties());
    }

    @Test
    void shouldNotRegisterPropertiesWhenNull() {
        KafkaStreamsExecutionContext.registerProperties(null);
        assertNull(KafkaStreamsExecutionContext.getProperties());
    }

    @Test
    void shouldAddPrefixToAppId() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        properties.put("prefix.self", "abc.");

        KafkaStreamsExecutionContext.registerProperties(properties);

        assertEquals("abc.", KafkaStreamsExecutionContext.getPrefix());
        assertEquals(
                "abc.appId", KafkaStreamsExecutionContext.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    @Test
    void shouldNotAddPrefixToAppIdIfNoPrefix() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");

        KafkaStreamsExecutionContext.registerProperties(properties);

        assertEquals("", KafkaStreamsExecutionContext.getPrefix());
        assertEquals("appId", KafkaStreamsExecutionContext.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    @Test
    void shouldNotAddPrefixToAppIdIfNotAppId() {
        Properties properties = new Properties();
        properties.put("prefix.self", "abc.");

        KafkaStreamsExecutionContext.registerProperties(properties);

        assertEquals("abc.", KafkaStreamsExecutionContext.getPrefix());
        assertNull(KafkaStreamsExecutionContext.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    @Test
    void shouldExtractDlqProperties() {
        Properties properties = new Properties();
        properties.put("dlq.some.feature", "true");
        properties.put("dlq.other.feature", "false");

        KafkaStreamsExecutionContext.registerProperties(properties);

        Properties dlqProps = KafkaStreamsExecutionContext.getDlqProperties();
        assertEquals("true", dlqProps.getProperty("dlq.some.feature"));
        assertEquals("false", dlqProps.getProperty("dlq.other.feature"));
    }

    @Test
    void shouldReturnTrueWhenDlqFeatureEnabled() {
        Properties properties = new Properties();
        properties.put("dlq.test.feature", "true");

        KafkaStreamsExecutionContext.registerProperties(properties);

        assertTrue(KafkaStreamsExecutionContext.isDlqFeatureEnabled("dlq.test.feature"));
    }

    @Test
    void shouldReturnFalseWhenDlqFeatureDisabled() {
        Properties properties = new Properties();
        properties.put("dlq.test.feature", "false");

        KafkaStreamsExecutionContext.registerProperties(properties);

        assertFalse(KafkaStreamsExecutionContext.isDlqFeatureEnabled("dlq.test.feature"));
    }

    @Test
    void shouldReturnFalseWhenDlqFeatureNotPresent() {
        Properties properties = new Properties();

        KafkaStreamsExecutionContext.registerProperties(properties);

        assertFalse(KafkaStreamsExecutionContext.isDlqFeatureEnabled("dlq.missing.feature"));
    }

    @Test
    void shouldUseDefaultFalseWhenDlqFeatureMissing() {
        KafkaStreamsExecutionContext.setDlqProperties(new Properties());
        assertFalse(KafkaStreamsExecutionContext.isDlqFeatureEnabled("dlq.non.existing"));
    }
}
