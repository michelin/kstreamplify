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

import com.michelin.kstreamplify.exception.ConfigFileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.yaml.snakeyaml.Yaml;

/** Properties utils. */
public final class PropertiesUtils {
    /** The Kafka properties prefix. */
    public static final String KAFKA_PROPERTIES_PREFIX = "kafka.properties.";

    /** The default config file. */
    public static final String CONFIG_FILE = "application.yml";

    /** Constructor. */
    private PropertiesUtils() {}

    /**
     * Load the properties from the config file.
     *
     * @return The properties
     */
    public static Properties loadProperties() {
        Yaml yaml = new Yaml();

        try (InputStream inputStream = PropertiesUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (inputStream == null) {
                throw new ConfigFileNotFoundException(CONFIG_FILE);
            }

            Map<String, Object> yamlProperties = yaml.load(inputStream);
            Properties properties = new Properties();
            if (yamlProperties != null) {
                flattenMap(yamlProperties, "", properties);
            }

            return properties;
        } catch (IOException e) {
            throw new ConfigFileNotFoundException(CONFIG_FILE, e);
        }
    }

    /**
     * Extract sub-properties by key prefix.
     *
     * @param properties The properties
     * @param keyPrefix The key prefix to filter by
     * @param removeKeyPrefix Whether to remove the key prefix from the extracted properties
     * @return The extracted sub-properties
     */
    public static Properties extractSubProperties(Properties properties, String keyPrefix, boolean removeKeyPrefix) {
        Properties kafkaProperties = new Properties();

        properties.stringPropertyNames().stream()
                .filter(key -> key.startsWith(keyPrefix))
                .forEach(key -> kafkaProperties.setProperty(
                        removeKeyPrefix ? key.substring(keyPrefix.length()) : key, properties.getProperty(key)));

        return kafkaProperties;
    }

    /**
     * Flatten a nested map into a flat properties object.
     *
     * @param map The map to flatten
     * @param prefix The prefix to use for the keys
     * @param out The output properties
     */
    @SuppressWarnings("unchecked")
    private static void flattenMap(Map<String, Object> map, String prefix, Properties out) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                flattenMap((Map<String, Object>) value, key + ".", out);
            } else if (value != null) {
                out.put(key, value);
            }
        }
    }

    /**
     * Check if a feature is enabled based on properties.
     *
     * @param properties The properties
     * @param key The property key
     * @param defaultValue The default value if the property is not set
     * @return true if the feature is enabled, false otherwise
     */
    public static boolean isFeatureEnabled(Properties properties, String key, boolean defaultValue) {
        return Boolean.parseBoolean(properties.getProperty(key, Boolean.toString(defaultValue)));
    }
}
