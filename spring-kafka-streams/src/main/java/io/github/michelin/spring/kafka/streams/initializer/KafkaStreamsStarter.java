package io.github.michelin.spring.kafka.streams.initializer;

import org.apache.kafka.streams.StreamsBuilder;

import static org.apache.commons.lang3.StringUtils.EMPTY;

public interface KafkaStreamsStarter {
    void topology(StreamsBuilder streamsBuilder);

    default String dlqTopic() { return EMPTY; }
}
