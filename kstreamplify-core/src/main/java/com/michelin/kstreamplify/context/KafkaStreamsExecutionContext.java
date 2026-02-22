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

import static com.michelin.kstreamplify.property.KstreamplifyConfig.DLQ_PROPERTIES_PREFIX;
import static com.michelin.kstreamplify.serde.TopicWithSerde.SELF;
import static com.michelin.kstreamplify.topic.TopicUtils.PREFIX_PROPERTY_NAME;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

import com.michelin.kstreamplify.property.PropertiesUtils;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/** The class to represent the context of the KStream. */
@Slf4j
@Getter
public class KafkaStreamsExecutionContext {
    @Getter
    @Setter
    private static String dlqTopicName;

    @Getter
    @Setter
    private static Map<String, String> serdesConfig;

    @Getter
    @Setter
    private static Properties properties;

    @Getter
    @Setter
    private static Properties dlqProperties;

    @Getter
    private static String prefix;

    /** Constructor. */
    private KafkaStreamsExecutionContext() {}

    /**
     * Register Kafka kafkaProperties.
     *
     * @param kafkaProperties The Kafka kafkaProperties
     */
    public static void registerProperties(Properties kafkaProperties) {
        if (kafkaProperties == null) {
            return;
        }

        prefix = kafkaProperties.getProperty(PREFIX_PROPERTY_NAME + "." + SELF, "");
        if (StringUtils.isNotBlank(prefix) && kafkaProperties.containsKey(APPLICATION_ID_CONFIG)) {
            kafkaProperties.setProperty(
                    APPLICATION_ID_CONFIG, prefix.concat(kafkaProperties.getProperty(APPLICATION_ID_CONFIG)));
        }

        properties = kafkaProperties;
        dlqProperties = PropertiesUtils.extractSubProperties(kafkaProperties, DLQ_PROPERTIES_PREFIX, false);
    }

    /**
     * Checks if a DLQ feature flag is enabled based on the given key.
     *
     * @param key The DLQ feature property key to check.
     * @return {@code true} if the feature is enabled; {@code false} otherwise.
     */
    public static boolean isDlqFeatureEnabled(String key) {
        return PropertiesUtils.isFeatureEnabled(dlqProperties, key, false);
    }
}
