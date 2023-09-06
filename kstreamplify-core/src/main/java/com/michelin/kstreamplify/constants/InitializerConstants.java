package com.michelin.kstreamplify.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Kafka Streams initialization constants
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class InitializerConstants {
    /**
     * Server port property name
     */
    public static final String SERVER_PORT_PROPERTY = "server.port";

    /**
     * Default host
     */
    public static final String LOCALHOST = "localhost";

    /**
     * Name of the property containing of the name of the var env containing the IP
     */
    public static final String IP_SYSTEM_VARIABLE_PROPERTY = "ip.env.var.name";

    /**
     * Default var env name containing the IP
     */
    public static final String IP_SYSTEM_VARIABLE_DEFAULT = "MY_POD_IP";
}
