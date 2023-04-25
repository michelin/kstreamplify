package io.github.michelin.spring.kafka.streams.jmx;

import javax.management.ObjectName;

public interface KafkaStreamStateStoreStatsMBean {

    long getStateStoreSize();
    ObjectName getName();

}