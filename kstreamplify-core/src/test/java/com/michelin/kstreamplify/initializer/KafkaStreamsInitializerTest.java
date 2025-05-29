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
package com.michelin.kstreamplify.initializer;

import static com.michelin.kstreamplify.initializer.KafkaStreamsInitializer.SERVER_PORT_PROPERTY_NAME;
import static com.michelin.kstreamplify.property.PropertiesUtils.KAFKA_PROPERTIES_PREFIX;
import static com.michelin.kstreamplify.property.PropertiesUtils.PROPERTY_SEPARATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.property.PropertiesUtils;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaStreamsInitializerTest {
    @Mock
    private KafkaStreamsStarter kafkaStreamsStarter;

    private final KafkaStreamsInitializer initializer = new KafkaStreamsInitializer(kafkaStreamsStarter);

    @Test
    void shouldStartProperties() {
        try (MockedStatic<PropertiesUtils> propertiesUtilsMockedStatic = mockStatic(PropertiesUtils.class)) {
            Properties properties = new Properties();
            properties.put(SERVER_PORT_PROPERTY_NAME, 8080);
            properties.put(KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + StreamsConfig.APPLICATION_ID_CONFIG, "appId");
            properties.put(KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + "prefix.self", "abc.");

            propertiesUtilsMockedStatic.when(PropertiesUtils::loadProperties).thenReturn(properties);

            propertiesUtilsMockedStatic
                    .when(() -> PropertiesUtils.loadKafkaProperties(any()))
                    .thenCallRealMethod();

            initializer.initProperties();

            assertNotNull(initializer.getProperties());
            assertEquals(8080, initializer.getServerPort());
            assertTrue(initializer.getKafkaProperties().containsKey(StreamsConfig.APPLICATION_ID_CONFIG));
            assertEquals("abc.", KafkaStreamsExecutionContext.getPrefix());
            assertEquals(
                    "abc.appId", KafkaStreamsExecutionContext.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));
        }
    }

    @Test
    void shouldShutdownClientOnUncaughtException() {
        try (MockedStatic<PropertiesUtils> propertiesUtilsMockedStatic = mockStatic(PropertiesUtils.class)) {
            Properties properties = new Properties();
            properties.put(SERVER_PORT_PROPERTY_NAME, 8080);
            properties.put(KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR + StreamsConfig.APPLICATION_ID_CONFIG, "appId");

            propertiesUtilsMockedStatic.when(PropertiesUtils::loadProperties).thenReturn(properties);

            propertiesUtilsMockedStatic
                    .when(() -> PropertiesUtils.loadKafkaProperties(any()))
                    .thenCallRealMethod();

            initializer.initProperties();

            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse response =
                    initializer.onStreamsUncaughtException(new RuntimeException("Test Exception"));

            assertEquals(StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT, response);
        }
    }
}
