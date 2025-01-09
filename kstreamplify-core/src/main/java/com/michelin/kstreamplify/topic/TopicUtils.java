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

package com.michelin.kstreamplify.topic;

import static com.michelin.kstreamplify.property.PropertiesUtils.PROPERTY_SEPARATOR;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import java.util.Properties;
import lombok.NoArgsConstructor;

/**
 * The topic utils class.
 */
@NoArgsConstructor(access = lombok.AccessLevel.PRIVATE)
public final class TopicUtils {
    /**
     * The topic property name.
     */
    public static final String TOPIC_PROPERTY_NAME = "topic";

    /**
     * The prefix property name.
     */
    public static final String PREFIX_PROPERTY_NAME = "prefix";

    /**
     * The remap property name.
     */
    public static final String REMAP_PROPERTY_NAME = "remap";

    /**
     * Prefix the given topic name with the configured prefix and applies the dynamic remap.
     * Prefix is retrieved at runtime from kafka.properties.prefix.[prefixPropertyKey].
     * <pre>{@code
     * kafka:
     *   properties:
     *     prefix:
     *       self: "myNamespacePrefix."
     * }</pre>
     * This allows interactions with multiple topics from different owners/namespaces.
     * If not provided, prefixing will not occur.
     * <br/>
     * Dynamic remap is retrieved from the configuration like so:
     * <pre>{@code
     * kafka:
     *   properties:
     *     topic:
     *       remap:
     *          myInitialTopicName: "myRemappedTopicName"
     * }</pre>
     * It can be applied to both input and output topics.
     *
     * @param topicName         The topicName that needs to be prefixed and remapped
     * @param prefixPropertyKey The prefixPropertyKey matching the configuration file
     * @return The prefixed and/or remapped topic.
     */
    public static String remapAndPrefix(String topicName, String prefixPropertyKey) {
        Properties properties = KafkaStreamsExecutionContext.getProperties();

        // Check for dynamic remap in properties
        String resultTopicName = properties.getProperty(TOPIC_PROPERTY_NAME
            + PROPERTY_SEPARATOR + REMAP_PROPERTY_NAME + PROPERTY_SEPARATOR + topicName, topicName);

        // Check if topic prefix property exists
        String prefix = properties.getProperty(PREFIX_PROPERTY_NAME + PROPERTY_SEPARATOR + prefixPropertyKey, "");
        return prefix.concat(resultTopicName);
    }
}
