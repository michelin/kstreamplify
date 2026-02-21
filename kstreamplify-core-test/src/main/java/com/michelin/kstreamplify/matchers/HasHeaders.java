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
package com.michelin.kstreamplify.matchers;

import com.michelin.kstreamplify.WithHeaders;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.core.IsEqual;

/**
 * A Hamcrest matcher that checks if a {@link WithHeaders} object has a header with a specific key and value.
 *
 * <p><b>Example:</b>
 *
 * <pre>{@code
 * // Assert that a record has a specific header
 * RecordHeaders headers = new RecordHeaders();
 * headers.add("request-id", "12345".getBytes(StandardCharsets.UTF_8));
 * KafkaRecord<String, String> record = new KafkaRecord<>(headers, "key", "value");
 *
 * MatcherAssert.assertThat(
 *     record,
 *     new HasHeaders("request-id", "12345".getBytes(StandardCharsets.UTF_8))
 * );
 *
 * // Use with HasRecord matcher to validate complete record
 * MatcherAssert.assertThat(
 *     record,
 *     new HasRecord<>(
 *         new HasHeaders("request-id", "12345".getBytes(StandardCharsets.UTF_8)),
 *         new HasKey<>("key"),
 *         new HasValue<>("value")
 *     )
 * );
 * }</pre>
 */
public final class HasHeaders extends TypeSafeMatcher<WithHeaders> {
    /** The key of the header to check. */
    private final String key;

    /** The matcher for the value of the header. */
    private final Matcher<byte[]> expected;

    /**
     * Constructs a HasHeaders matcher with the given key and expected value matcher.
     *
     * @param key The key of the header to check
     * @param expected The matcher for the value of the header
     */
    public HasHeaders(final String key, final Matcher<byte[]> expected) {
        this.key = key;
        this.expected = expected;
    }

    /**
     * Constructs a HasHeaders matcher with the given key and expected value.
     *
     * @param key The key of the header to check
     * @param expected The expected value of the header
     */
    public HasHeaders(final byte[] expected, final String key) {
        this(key, new IsEqual<>(expected));
    }

    @Override
    protected boolean matchesSafely(final WithHeaders actual) {
        final Headers headers = actual.headers();
        boolean matches = false;
        if (headers != null) {
            final Header header = headers.lastHeader(this.key);
            if (header != null) {
                matches = this.expected.matches(header.value());
            }
        }
        return matches;
    }

    @Override
    public void describeTo(final Description description) {
        description
                .appendText("a TestRecord containing header ")
                .appendValue(this.key)
                .appendText(" with value ");
        this.expected.describeTo(description);
    }

    @Override
    protected void describeMismatchSafely(final WithHeaders actual, final Description description) {
        final Header header = actual.headers().lastHeader(this.key);
        if (header == null) {
            description.appendText("header was missing");
        } else {
            description.appendText("value was ").appendValue(new String(header.value(), StandardCharsets.UTF_8));
        }
    }
}
