package io.github.michelin.spring.kafka.streams.model;

/**
 * The topology type enum
 */
public enum TopologyObjectType {
    /**
     * The input topic type
     */
    TOPIC_IN,

    /**
     * The output topic type
     */
    TOPIC_OUT,

    /**
     * The stream type
     */
    STREAM;
}
