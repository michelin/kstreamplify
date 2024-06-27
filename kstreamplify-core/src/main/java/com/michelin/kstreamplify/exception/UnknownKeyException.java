package com.michelin.kstreamplify.exception;

/**
 * Exception thrown when a key is not found.
 */
public class UnknownKeyException extends RuntimeException {
    private static final String UNKNOWN_KEY = "Key %s not found";

    /**
     * Constructor.
     *
     * @param key The key that was not found
     */
    public UnknownKeyException(String key) {
        super(String.format(UNKNOWN_KEY, key));
    }
}
