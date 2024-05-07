package com.michelin.kstreamplify.http.exception;

/**
 * Store not found exception.
 */
public class StoreNotFoundException extends RuntimeException {
    private static final String STORE_NOT_FOUND = "State store %s not found";

    /**
     * Constructor.
     *
     * @param store The store
     */
    public StoreNotFoundException(String store) {
        super(String.format(STORE_NOT_FOUND, store));
    }
}
