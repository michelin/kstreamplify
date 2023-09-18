package com.michelin.kstreamplify.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * HTTP server constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HttpServerConstants {
    /**
     * Readiness probe path property name.
     */
    public static final String READINESS_PROPERTY = "readiness_path";

    /**
     * Liveness probe path property name.
     */
    public static final String LIVENESS_PROPERTY = "liveness_path";

    /**
     * Topology property name.
     */
    public static final String TOPOLOGY_PROPERTY = "expose_topology_path";

    /**
     * Readiness default path.
     */
    public static final String READINESS_DEFAULT_PATH = "ready";

    /**
     * Liveness default path.
     */
    public static final String LIVENESS_DEFAULT_PATH = "liveness";

    /**
     * Topology default path.
     */
    public static final String TOPOLOGY_DEFAULT_PATH = "topology";
}
