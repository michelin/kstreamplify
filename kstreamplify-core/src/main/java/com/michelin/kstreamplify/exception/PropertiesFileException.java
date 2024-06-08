package com.michelin.kstreamplify.exception;

/**
 * Exception thrown when a properties file cannot be read.
 */
public class PropertiesFileException extends RuntimeException {
    private static final String CANNOT_READ_PROPERTIES_FILE = "Cannot read properties file";

    /**
     * Constructor.
     *
     * @param cause The cause of the exception
     */
    public PropertiesFileException(Throwable cause) {
        super(CANNOT_READ_PROPERTIES_FILE, cause);
    }
}
