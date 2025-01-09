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

import static com.michelin.kstreamplify.property.PropertiesUtils.PROPERTY_SEPARATOR;
import static com.michelin.kstreamplify.serde.TopicWithSerde.SELF;
import static com.michelin.kstreamplify.topic.TopicUtils.PREFIX_PROPERTY_NAME;

import java.util.Map;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.StreamsConfig;

/**
 * The class to represent the context of the KStream.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsExecutionContext {

    /**
     * The DLQ topic name.
     */
    @Getter
    @Setter
    private static String dlqTopicName;

    /**
     * The Serdes config Map.
     */
    @Getter
    @Setter
    private static Map<String, String> serdesConfig;

    /**
     * The properties of the stream execution context.
     */
    @Getter
    @Setter
    private static Properties properties;

    /**
     * The prefix that will be applied to the application.id if provided.
     * It needs to be defined like this:
     * <pre>{@code
     * kafka:
     *   properties:
     *     prefix:
     *       self: "myNamespacePrefix."
     * }</pre>
     */
    @Getter
    private static String prefix;

    /**
     * Register KStream properties.
     *
     * @param properties The Kafka Streams properties
     */
    public static void registerProperties(Properties properties) {
        if (properties == null) {
            return;
        }

        prefix = properties.getProperty(PREFIX_PROPERTY_NAME + PROPERTY_SEPARATOR + SELF, "");
        if (StringUtils.isNotBlank(prefix)
            && properties.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
            properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
                prefix.concat(properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)));
        }

        KafkaStreamsExecutionContext.properties = properties;
    }
}
