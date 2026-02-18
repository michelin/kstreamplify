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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.cactoos.list.ListOf;
import org.cactoos.map.MapOf;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A configuration source, providing properties as a {@link Properties} object.
 */
@FunctionalInterface
public interface Configuration {
    /**
     * Provides the configuration properties.
     *
     * @return The configuration properties.
     */
    Properties properties();

    /**
     * An envelope for a configuration, allowing to override or extend the properties of the wrapped configuration.
     */
    abstract class Envelope implements Configuration {
        /**
         * The wrapped configuration.
         */
        private final Configuration wrapped;

        /**
         * Ctor.
         *
         * @param wrapped The configuration to wrap.
         */
        protected Envelope(final Configuration wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Properties properties() {
            return this.wrapped.properties();
        }
    }

    /**
     * A simple configuration source, providing properties from a given {@link Properties} object.
     */
    final class Simple implements Configuration {

        /**
         * The properties provided by this configuration.
         */
        private final Properties props;

        /**
         * Ctor.
         *
         * @param props The properties provided by this configuration.
         */
        public Simple(final Properties props) {
            this.props = props;
        }

        @Override
        public Properties properties() {
            return this.props;
        }
    }

    /**
     * A configuration source that loads properties from a file in the classpath.
     *
     * <p><b>Example:</b></p>
     * <pre>{@code
     * // Load configuration from a properties file in the classpath
     * Configuration config = new Configuration.FromFile("application-test.properties");
     *
     * // Use the loaded configuration
     * Properties props = config.properties();
     * String appName = props.getProperty("app.name");
     * }</pre>
     */
    final class FromFile implements Configuration {
        /**
         * The name of the file to load, relative to the classpath.
         */
        private final String fileName;

        /**
         * Ctor.
         *
         * @param fileName The name of the file to load, relative to the classpath.
         */
        public FromFile(final String fileName) {
            this.fileName = fileName;
        }

        @Override
        public Properties properties() {
            try (InputStream input = Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream(this.fileName)) {
                if (input == null) {
                    throw new IllegalArgumentException(String.format("Resource not found: %s", this.fileName));
                }
                final Properties properties = new Properties();
                properties.load(input);
                return properties;
            } catch (final IOException exception) {
                throw new IllegalStateException(
                        String.format("Failed to load resource: %s", this.fileName),
                        exception
                );
            }
        }
    }

    /**
     * A configuration source that loads properties from a given map of string keys and values.
     *
     * <p><b>Example:</b></p>
     * <pre>{@code
     * // Load configuration from map entries
     * Configuration config = new Configuration.FromMap(
     *     new MapEntry<>("key.test", "value.test"),
     *     new MapEntry<>("app.name", "MyApp")
     * );
     *
     * // Or from a map
     * Map<String, String> configMap = new HashMap<>();
     * configMap.put("db.host", "localhost");
     * configMap.put("db.port", "5432");
     * Configuration config = new Configuration.FromMap(configMap);
     *
     * Properties props = config.properties();
     * }</pre>
     */
    final class FromMap extends Envelope {
        /**
         * Primary ctor.
         *
         * @param wrapped The configuration to wrap.
         */
        public FromMap(final Configuration wrapped) {
            super(wrapped);
        }

        /**
         * Secondary ctor.
         *
         * @param props The properties to load.
         */
        public FromMap(final Properties props) {
            this(new Simple(props));
        }

        /**
         * Secondary ctor.
         *
         * @param map The map of string keys and values to load.
         */
        public FromMap(final Map<String, String> map) {
            this(
                    map.entrySet()
                            .stream()
                            .collect(
                                    Properties::new,
                                    (final Properties props, final Map.Entry<String, String> entry) -> props.setProperty(entry.getKey(), entry.getValue()),
                                    Properties::putAll
                            )
            );
        }

        /**
         * Secondary ctor.
         *
         * @param entries The entries of the map of string keys and values to load.
         */
        @SafeVarargs
        public FromMap(final Map.Entry<String, String>... entries) {
            this(new MapOf<>(entries));
        }
    }

    /**
     * A configuration source that loads properties from a HOCON configuration string or object.
     *
     * <p><b>Example:</b></p>
     * <pre>{@code
     * // Load configuration from a HOCON string
     * Configuration config = new Configuration.FromHocon("app.name = MyApp\napp.version = 1.0");
     *
     * // Or load from a HOCON file
     * Config hoconConfig = ConfigFactory.parseResources("config/application.conf").resolve();
     * Configuration config = new Configuration.FromHocon(hoconConfig);
     *
     * Properties props = config.properties();
     * String appName = props.getProperty("app.name");
     * }</pre>
     */
    final class FromHocon extends Envelope {

        /**
         * Primary ctor.
         *
         * @param wrapped The configuration to wrap.
         */
        public FromHocon(final Configuration wrapped) {
            super(wrapped);
        }

        /**
         * Secondary ctor.
         *
         * @param props The properties to load.
         */
        public FromHocon(final Properties props) {
            this(new Simple(props));
        }

        /**
         * Secondary ctor.
         *
         * @param config The HOCON configuration to load.
         */
        public FromHocon(final Config config) {
            this(
                    config.entrySet()
                            .stream()
                            .collect(
                                    Properties::new,
                                    (final Properties props, final Map.Entry<String, ConfigValue> entry) -> props.setProperty(
                                            entry.getKey(),
                                            String.valueOf(entry.getValue().unwrapped())
                                    ),
                                    Properties::putAll
                            )
            );
        }

        /**
         * Secondary ctor.
         *
         * @param content The HOCON configuration string to load.
         */
        public FromHocon(final String content) {
            this(ConfigFactory.parseString(content).resolve());
        }
    }

    /**
     * A configuration source that overrides the properties of a wrapped configuration with the properties of another configuration or map.
     *
     * <p><b>Example:</b></p>
     * <pre>{@code
     * // Create a base configuration and override specific values
     * Configuration baseConfig = new Configuration.FromMap(
     *     new MapEntry<>("key.test", "original.value"),
     *     new MapEntry<>("other.key", "other.value")
     * );
     *
     * Configuration overriddenConfig = new Configuration.Overridden(
     *     baseConfig,
     *     new MapEntry<>("key.test", "overridden.value")
     * );
     *
     * Properties props = overriddenConfig.properties();
     * String value = props.getProperty("key.test"); // Returns "overridden.value"
     * String other = props.getProperty("other.key"); // Returns "other.value"
     * }</pre>
     */
    final class Overridden extends Envelope {

        /**
         * The configuration providing the properties to override the wrapped configuration.
         */
        private final Configuration override;

        /**
         * Primary ctor.
         *
         * @param base     The configuration to wrap.
         * @param override The configuration providing the properties to override the wrapped configuration.
         */
        public Overridden(final Configuration base, final Configuration override) {
            super(base);
            this.override = override;
        }

        /**
         * Secondary ctor.
         *
         * @param base The configuration to wrap.
         * @param map  The map of string keys and values providing the properties to override the wrapped configuration.
         */
        public Overridden(final Configuration base, final Map<String, String> map) {
            this(base, new FromMap(map));
        }

        /**
         * Secondary ctor.
         *
         * @param base    The configuration to wrap.
         * @param entries The entries of the map of string keys and values providing the properties to override the wrapped configuration.
         */
        @SafeVarargs
        public Overridden(final Configuration base, final Map.Entry<String, String>... entries) {
            this(base, new MapOf<>(entries));
        }

        @Override
        public Properties properties() {
            final Properties base = super.properties();
            base.putAll(this.override.properties());
            return base;
        }
    }

    /**
     * A configuration source that loads properties from the environment variables, filtered by a list of keys.
     */
    final class FromEnvironment implements Configuration {

        /**
         * The list of keys to filter the environment variables.
         */
        private final List<String> keys;

        /**
         * Primary ctor.
         *
         * @param keys The list of keys to filter the environment variables.
         */
        public FromEnvironment(final List<String> keys) {
            this.keys = keys;
        }

        /**
         * Secondary ctor.
         *
         * @param keys The keys to filter the environment variables.
         */
        public FromEnvironment(final String... keys) {
            this(new ListOf<>(keys));
        }

        @Override
        public Properties properties() {
            final Map<String, String> env = System.getenv();
            return this.keys.stream()
                    .filter(env::containsKey)
                    .collect(
                            Properties::new,
                            (final Properties props, final String key) -> props.setProperty(key, env.get(key)),
                            Properties::putAll
                    );
        }
    }
}
