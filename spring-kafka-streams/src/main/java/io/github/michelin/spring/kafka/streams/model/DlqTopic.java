package io.github.michelin.spring.kafka.streams.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DlqTopic {
    private String name;
}
