package com.michelin.kstreamplify.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Property constants
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PropertyConstants {
    /**
     * Property separator
     */
    public static final String PROPERTY_SEPARATOR = ".";

    /**
     * Kafka properties prefix
     */
    public static final String KAFKA_PROPERTIES_PREFIX = "kafka.properties";

    /**
     * Default property file name
     */
    public static final String DEFAULT_PROPERTY_FILE = "application.yml";

    /**
     * Prefix property name
     */
    public static final String PREFIX_PROPERTY_NAME = "prefix";

    /**
     * Topic property name
     */
    public static final String TOPIC_PROPERTY_NAME = "topic";

    /**
     * Remap property name
     */
    public static final String REMAP_PROPERTY_NAME = "remap";
}
