package com.michelin.kstreamplify.exception;

/**
 * Exception thrown when a response from another instance cannot be read.
 */
public class OtherInstanceResponseException extends RuntimeException {
    private static final String OTHER_INSTANCE_RESPONSE = "Fail to read other instance response";

    /**
     * Constructor.
     *
     * @param cause The cause of the exception
     */
    public OtherInstanceResponseException(Throwable cause) {
        super(OTHER_INSTANCE_RESPONSE, cause);
    }
}
