package io.github.michelin.spring.kafka.streams.model;

import lombok.Builder;
import lombok.Getter;

/**
 * The dead letter queue (DLQ) topic
 */
@Getter
@Builder
public class DlqTopic {
    /**
     * The DLQ topic name
     */
    private String name;
}
