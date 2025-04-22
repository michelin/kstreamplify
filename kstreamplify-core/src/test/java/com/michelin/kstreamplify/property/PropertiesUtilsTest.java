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

import static com.michelin.kstreamplify.property.PropertiesUtils.KAFKA_PROPERTIES_PREFIX;
import static com.michelin.kstreamplify.property.PropertiesUtils.PROPERTY_SEPARATOR;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class PropertiesUtilsTest {

    @Test
    void shouldLoadProperties() {
        Properties properties = PropertiesUtils.loadProperties();

        assertTrue(properties.containsKey("server.port"));
        assertTrue(properties.containsValue(8080));

        assertTrue(properties.containsKey(KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + APPLICATION_ID_CONFIG));
        assertTrue(properties.containsValue("appId"));
    }

    @Test
    void shouldLoadKafkaProperties() {
        Properties properties = PropertiesUtils.loadKafkaProperties(PropertiesUtils.loadProperties());

        assertTrue(properties.containsKey(APPLICATION_ID_CONFIG));
        assertTrue(properties.containsValue("appId"));
    }

    @Test
    void shouldRemoveKafkaPrefix() {
        Properties prop = new Properties();
        prop.put(KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + "kafkaProp", "propValue");
        Properties resultProperties = PropertiesUtils.removeKafkaPrefix(prop);

        assertTrue(resultProperties.containsKey("kafkaProp"));
        assertTrue(resultProperties.containsValue("propValue"));
        assertFalse(resultProperties.containsKey(KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + "kafkaProp"));
    }

    @Test
    void shouldNotRemoveKafkaPrefix() {
        Properties prop = new Properties();
        prop.put("another.properties.prop", "propValue");
        Properties resultProperties = PropertiesUtils.removeKafkaPrefix(prop);

        assertTrue(resultProperties.containsKey("another.properties.prop"));
        assertTrue(resultProperties.containsValue("propValue"));
        assertFalse(resultProperties.containsKey("prop"));
    }

    @Test
    void shouldNotRemoveKafkaPropertiesStringWhenNotPrefix() {
        Properties prop = new Properties();
        prop.put("prefix." + KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + "kafkaProp", "propValue");
        Properties resultProperties = PropertiesUtils.removeKafkaPrefix(prop);

        assertTrue(
                resultProperties.containsKey("prefix." + KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + "kafkaProp"));
        assertTrue(resultProperties.containsValue("propValue"));
        assertFalse(resultProperties.containsKey("prefix.kafkaProp"));
    }
}
