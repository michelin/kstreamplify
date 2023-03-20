package com.michelin.kafka.streams.init.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DlqTopic {
    private String name;
}
