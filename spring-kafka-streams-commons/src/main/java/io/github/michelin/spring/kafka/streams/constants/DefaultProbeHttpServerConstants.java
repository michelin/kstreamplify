package io.github.michelin.spring.kafka.streams.constants;

public abstract class DefaultProbeHttpServerConstants {

    public static final String READINESS_PROPERTY = "readiness_path";
    public static final String LIVENESS_PROPERTY = "liveness_path";
    public static final String TOPOLOGY_PROPERTY = "expose_topology_path";
    public static final String READINESS_DEFAULT_PATH = "ready";
    public static final String LIVENESS_DEFAULT_PATH = "liveness";
    public static final String TOPOLOGY_DEFAULT_PATH = "topology";
    
    
}
