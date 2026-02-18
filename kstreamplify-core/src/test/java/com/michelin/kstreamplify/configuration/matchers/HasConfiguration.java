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
package com.michelin.kstreamplify.configuration.matchers;

import com.michelin.kstreamplify.configuration.Configuration;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Properties;

/**
 * A matcher for {@link Configuration} objects that checks if their properties match a given matcher.
 *
 * <p><b>Example:</b></p>
 * <pre>{@code
 * // Assert that a configuration contains specific properties
 * Configuration config = new Configuration.FromMap(
 *     new MapEntry<>("app.name", "MyApp"),
 *     new MapEntry<>("app.version", "1.0.0")
 * );
 *
 * MatcherAssert.assertThat(
 *     config,
 *     new HasConfiguration(
 *         new HasProperty("app.name", "MyApp")
 *     )
 * );
 *
 * // Or combine multiple property matchers
 * MatcherAssert.assertThat(
 *     config,
 *     new HasConfiguration(
 *         new AllOf<>(
 *             new HasProperty("app.name", "MyApp"),
 *             new HasProperty("app.version", "1.0.0")
 *         )
 *     )
 * );
 * }</pre>
 */
public final class HasConfiguration extends TypeSafeMatcher<Configuration> {

    /**
     * The matcher for the properties of the configuration.
     */
    private final Matcher<Properties> properties;

    /**
     * Constructs a HasConfiguration matcher with the given properties matcher.
     *
     * @param properties The matcher for the properties of the configuration
     */
    public HasConfiguration(final Matcher<Properties> properties) {
        this.properties = properties;
    }

    @Override
    protected boolean matchesSafely(final Configuration configuration) {
        return this.properties.matches(configuration.properties());
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("a Configuration matching ");
        this.properties.describeTo(description);
    }

    @Override
    protected void describeMismatchSafely(final Configuration configuration, final Description description) {
        description.appendText("was ")
                .appendValue(configuration.properties());
    }
}
