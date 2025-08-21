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

import com.michelin.kstreamplify.exception.PropertiesFileException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

/** Properties utils. */
public final class PropertiesUtils {
    /** The Kafka properties prefix. */
    public static final String KAFKA_PROPERTIES_PREFIX = "kafka.properties";

    /** The default property file. */
    public static final String DEFAULT_PROPERTY_FILE = "application.yml";

    /** The property separator. */
    public static final String PROPERTY_SEPARATOR = ".";

    /** The Dlq properties prefix. */
    public static final String DLQ_PROPERTIES_PREFIX = "dlq";

    private PropertiesUtils() {}

    /**
     * Load the properties from the default properties file.
     *
     * @return The properties
     */
    public static Properties loadProperties() {
        Yaml yaml = new Yaml();

        try (InputStream inputStream =
                PropertiesUtils.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTY_FILE)) {
            LinkedHashMap<String, Object> propsMap = yaml.load(inputStream);
            return parsePropertiesMap(propsMap);
        } catch (IOException e) {
            throw new PropertiesFileException(e);
        }
    }

    /**
     * Get the Kafka properties only from the given properties.
     *
     * @param props The properties
     * @return The Kafka properties
     */
    public static Properties loadKafkaProperties(Properties props) {
        Properties resultProperties = new Properties();
        for (var prop : props.entrySet()) {
            if (StringUtils.contains(prop.getKey().toString(), KAFKA_PROPERTIES_PREFIX)) {
                resultProperties.put(
                        StringUtils.remove(prop.getKey().toString(), KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR),
                        prop.getValue());
            }
        }
        return resultProperties;
    }

    /**
     * Parse a map into Properties.
     *
     * @param map The map
     * @return The properties
     */
    private static Properties parsePropertiesMap(LinkedHashMap<String, Object> map) {
        return parseKey("", map, null);
    }

    /**
     * Parse a given key.
     *
     * @param key The key
     * @param map The underlying map
     * @param properties The properties
     * @return The properties
     */
    private static Properties parseKey(String key, Object map, Properties properties) {
        if (properties == null) {
            properties = new Properties();
        }

        String separator = PROPERTY_SEPARATOR;
        if (StringUtils.isBlank(key)) {
            separator = "";
        }

        if (map instanceof LinkedHashMap) {
            for (Object mapKey : ((LinkedHashMap<?, ?>) map).keySet()) {
                parseKey(key + separator + mapKey, ((LinkedHashMap<?, ?>) map).get(mapKey), properties);
            }
        } else {
            properties.put(key, map);
        }
        return properties;
    }

    /**
     * Extract properties by prefix.
     *
     * @param properties The properties
     * @param prefix The prefix to filter by
     * @return The filtered properties
     */
    public static Properties extractPropertiesByPrefix(Properties properties, String prefix) {
        Properties result = new Properties();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                result.setProperty(key, properties.getProperty(key));
            }
        }
        return result;
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
