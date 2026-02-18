/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.configuration;

import java.util.Map;
import java.util.Properties;
import org.cactoos.map.MapOf;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;

/** A {@link Configuration} that sets environment variables before returning the properties. */
public class ConfigurationWithEnvVars extends Configuration.Envelope {

    /** The environment variables to set. */
    private final EnvironmentVariables envvars;

    /** The map of environment variables to set. */
    private final Map<String, String> map;

    /**
     * Constructs a ConfigurationWithEnvVars with the given wrapped configuration, environment variables, and map of
     * environment variables to set.
     *
     * @param wrapped The wrapped configuration
     * @param envvars The environment variables to set
     * @param map The map of environment variables to set
     */
    public ConfigurationWithEnvVars(
            final Configuration wrapped, final EnvironmentVariables envvars, final Map<String, String> map) {
        super(wrapped);
        this.envvars = envvars;
        this.map = map;
    }

    /**
     * Constructs a ConfigurationWithEnvVars with the given wrapped configuration, environment variables, and map of
     * environment variables to set.
     *
     * @param wrapped The wrapped configuration
     * @param envvars The environment variables to set
     * @param map The map of environment variables to set
     */
    @SafeVarargs
    public ConfigurationWithEnvVars(
            final Configuration wrapped, final EnvironmentVariables envvars, final Map.Entry<String, String>... map) {
        this(wrapped, envvars, new MapOf<>(map));
    }

    @Override
    public Properties properties() {
        this.map.forEach(this.envvars::set);
        return super.properties();
    }
}
