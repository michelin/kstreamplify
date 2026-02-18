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

import com.michelin.kstreamplify.configuration.matchers.HasConfiguration;
import com.michelin.kstreamplify.configuration.matchers.HasProperty;
import com.typesafe.config.ConfigFactory;
import org.cactoos.map.MapEntry;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.AllOf;
import org.hamcrest.text.IsEqualIgnoringCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@ExtendWith(SystemStubsExtension.class)
final class ConfigurationTest {

    @SystemStub
    private final EnvironmentVariables envvars = new EnvironmentVariables();

    @Test
    void isFromFileOk() {
        MatcherAssert.assertThat(
                "A Configuration from file contains value for key",
                new Configuration.FromFile(
                        "application-test.properties"
                ),
                new HasConfiguration(
                        new AllOf<>(
                                new HasProperty("app.name", "TestApp"),
                                new HasProperty("app.version", "2.0.0")
                        )
                )
        );
    }

    @Test
    void isFromFileThrowsExceptionIfFileNotFound() {
        MatcherAssert.assertThat(
                "A Configuration from file throws exception if file not found",
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new Configuration.FromFile(
                                "non-existing-file.properties"
                        ).properties()
                ).getMessage(),
                new IsEqualIgnoringCase("Resource not found: non-existing-file.properties")
        );
    }

    @Test
    void isFromMapOk() {
        MatcherAssert.assertThat(
                "A Configuration from Map contains value for key",
                new Configuration.FromMap(
                        new MapEntry<>("key.test", "value.test")
                ),
                new HasConfiguration(
                        new HasProperty("key.test", "value.test")
                )
        );
    }

    @Test
    void isFromHoconFromStringOk() {
        MatcherAssert.assertThat(
                "A Configuration from Hocon string contains value for key",
                new Configuration.FromHocon(
                        "key.test = value.test"
                ),
                new HasConfiguration(
                        new HasProperty("key.test", "value.test")
                )
        );
    }

    @Test
    void isFromHoconFromFileOk() {
        MatcherAssert.assertThat(
                "A Configuration from Hocon file contains value for key",
                new Configuration.FromHocon(
                        ConfigFactory.parseResources("config/application-test.conf").resolve()
                ),
                new HasConfiguration(
                        new HasProperty("app.kafka.application-id", "test-app")
                )
        );
    }

    @Test
    void isOverriddenOk() {
        MatcherAssert.assertThat(
                "A Configuration overridden contains new value for key",
                new Configuration.Overridden(
                        new Configuration.FromMap(
                                new MapEntry<>("key.test", "value.test1")
                        ),
                        new MapEntry<>("key.test", "value.test2")
                ),
                new HasConfiguration(
                        new HasProperty("key.test", "value.test2")
                )
        );
    }

    @Test
    void isFromEnvOk() {
        MatcherAssert.assertThat(
                "A Configuration from environment contains new value for key",
                new ConfigurationWithEnvVars(
                        new Configuration.FromEnvironment(
                                "ENV_VAR"
                        ),
                        this.envvars,
                        new MapEntry<>("ENV_VAR", "env.value")
                ),
                new HasConfiguration(
                        new HasProperty("ENV_VAR", "env.value")
                )
        );
    }
}
