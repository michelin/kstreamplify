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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsExecutionContextTest {

    @BeforeEach
    void setUp() {
        KafkaStreamsExecutionContext.setProperties(null);
    }

    @Test
    void shouldNotRegisterPropertiesWhenNull() {
        KafkaStreamsExecutionContext.registerProperties(null, null);
        assertNull(KafkaStreamsExecutionContext.getProperties());
        assertNull(KafkaStreamsExecutionContext.getKafkaProperties());
    }

    @Test
    void shouldAddPrefixToAppId() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        properties.put("prefix.self", "abc.");

        KafkaStreamsExecutionContext.registerProperties(properties, null);

        assertEquals("abc.", KafkaStreamsExecutionContext.getPrefix());
        assertEquals(
                "abc.appId", KafkaStreamsExecutionContext.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    @Test
    void shouldNotAddPrefixToAppIdIfNoPrefix() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");

        KafkaStreamsExecutionContext.registerProperties(properties, null);

        assertEquals("", KafkaStreamsExecutionContext.getPrefix());
        assertEquals("appId", KafkaStreamsExecutionContext.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    @Test
    void shouldNotAddPrefixToAppIdIfNotAppId() {
        Properties properties = new Properties();
        properties.put("prefix.self", "abc.");

        KafkaStreamsExecutionContext.registerProperties(properties, null);

        assertEquals("abc.", KafkaStreamsExecutionContext.getPrefix());
        assertNull(KafkaStreamsExecutionContext.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));
    }

    @Test
    void shouldRegisterNonKafkaPropertiesInProperties() {
        Properties properties = new Properties();
        properties.put("notKafka.properties.prop", "propValue");

        KafkaStreamsExecutionContext.registerProperties(properties, null);

        assertEquals("propValue", KafkaStreamsExecutionContext.getProperties().get("notKafka.properties.prop"));
        assertNull(KafkaStreamsExecutionContext.getKafkaProperties());
    }

    @Test
    void shouldRegisterKafkaPropertiesInProperties() {
        Properties properties = new Properties();
        properties.put("kafka.properties.kafkaProp", "kafkaPropValue");

        KafkaStreamsExecutionContext.registerProperties(properties, null);

        assertEquals("kafkaPropValue", KafkaStreamsExecutionContext.getProperties().get("kafkaProp"));
        assertNull(KafkaStreamsExecutionContext.getProperties().get("kafka.properties.kafkaProp"));
    }

    @Test
    void shouldRegisterKafkaPropertiesInKafkaProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("kafkaProp", "propValue");

        KafkaStreamsExecutionContext.registerProperties(new Properties(), kafkaProperties);

        assertEquals("propValue", KafkaStreamsExecutionContext.getKafkaProperties().get("kafkaProp"));
    }
}
