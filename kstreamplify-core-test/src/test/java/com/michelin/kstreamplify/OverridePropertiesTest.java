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

package com.michelin.kstreamplify;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OverridePropertiesTest extends KafkaStreamsStarterTest {

    private static final String DLQ_TOPIC = "dlqTopic";
    private static final String SPECIFIC_STORAGE_PATH = "/tmp/PersonalPath";
    private static final String SPECIFIC_SCHEMA_REGISTRY_URL = "mock://specific-schema-registry-url";

    @Override
    protected KafkaStreamsStarter getKafkaStreamsStarter() {
        return new KafkaStreamsStarter() {
            @Override
            public String dlqTopic() {
                return DLQ_TOPIC;
            }

            @Override
            public void topology(StreamsBuilder streamsBuilder) {
                // Do nothing
            }
        };
    }

    /**
     * Overwrite the default storage path.
     *
     * @return the new properties
     */
    @Override
    protected Map<String, String> getSpecificProperties() {
        return Map.of(
            STATE_DIR_CONFIG, SPECIFIC_STORAGE_PATH,
                SCHEMA_REGISTRY_URL_CONFIG, SPECIFIC_SCHEMA_REGISTRY_URL
        );
    }

    /**
     * Test when default properties are overridden.
     */
    @Test
    void shouldValidateOverriddenProperties() {
        Properties properties = KafkaStreamsExecutionContext.getProperties();
        Assertions.assertEquals(SPECIFIC_STORAGE_PATH, properties.getProperty(STATE_DIR_CONFIG));
        Assertions.assertEquals(SPECIFIC_SCHEMA_REGISTRY_URL, properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
    }
}