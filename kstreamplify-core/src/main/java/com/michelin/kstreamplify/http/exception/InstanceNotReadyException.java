package com.michelin.kstreamplify.http.exception;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Instance not ready exception.
 */
public class InstanceNotReadyException extends RuntimeException {
    private static final String INSTANCE_NOT_READY = "Cannot process request while instance is in %s state";

    /**
     * Constructor.
     *
     * @param state The state
     */
    public InstanceNotReadyException(KafkaStreams.State state) {
        super(String.format(INSTANCE_NOT_READY, state));
    }
}
