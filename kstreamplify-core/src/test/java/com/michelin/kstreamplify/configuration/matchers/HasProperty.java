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

import java.util.Properties;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.text.IsEqualIgnoringCase;

/**
 * A Hamcrest matcher that checks if a {@link Properties} object has a property with a specific key and value.
 *
 * <p><b>Example:</b>
 *
 * <pre>{@code
 * // Assert that properties contain specific key-value pairs
 * Properties props = new Properties();
 * props.setProperty("db.host", "localhost");
 * props.setProperty("db.port", "5432");
 *
 * MatcherAssert.assertThat(
 *     props,
 *     new HasProperty("db.host", "localhost")
 * );
 *
 * // Use with Configuration matcher
 * Configuration config = new Configuration.FromMap(
 *     new MapEntry<>("app.name", "TestApp"),
 *     new MapEntry<>("app.version", "2.0.0")
 * );
 *
 * MatcherAssert.assertThat(
 *     config,
 *     new HasConfiguration(
 *         new AllOf<>(
 *             new HasProperty("app.name", "TestApp"),
 *             new HasProperty("app.version", "2.0.0")
 *         )
 *     )
 * );
 * }</pre>
 */
public final class HasProperty extends TypeSafeMatcher<Properties> {

    /** The key of the property to check. */
    private final String key;

    /** The matcher for the value of the property. */
    private final Matcher<String> expected;

    /**
     * Constructs a HasProperty matcher with the given key and expected value matcher.
     *
     * @param key The key of the property to check
     * @param expected The matcher for the value of the property
     */
    public HasProperty(final String key, final Matcher<String> expected) {
        this.key = key;
        this.expected = expected;
    }

    /**
     * Constructs a HasProperty matcher with the given key and expected value.
     *
     * @param key The key of the property to check
     * @param expected The expected value of the property
     */
    public HasProperty(final String key, final String expected) {
        this(key, new IsEqualIgnoringCase(expected));
    }

    @Override
    protected boolean matchesSafely(final Properties properties) {
        return this.expected.matches(properties.getProperty(this.key));
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("a Properties matching ");
        this.expected.describeTo(description);
        description.appendText(" for key ").appendValue(this.key);
    }

    @Override
    protected void describeMismatchSafely(final Properties properties, final Description description) {
        description.appendText("was ").appendValue(properties.getProperty(this.key));
    }
}
