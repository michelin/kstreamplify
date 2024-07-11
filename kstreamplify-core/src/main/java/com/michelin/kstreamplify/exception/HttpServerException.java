package com.michelin.kstreamplify.exception;

/**
 * Exception thrown when the HTTP server cannot be created.
 */
public class HttpServerException extends RuntimeException {
    private static final String FAIL_TO_CREATE = "Fail to create the HTTP server";

    /**
     * Constructor.
     *
     * @param cause The cause of the exception
     */
    public HttpServerException(Throwable cause) {
        super(FAIL_TO_CREATE, cause);
    }
}
