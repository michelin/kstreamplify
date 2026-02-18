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
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * A Hamcrest matcher that ignores the headers of a {@link WithHeaders} object.
 *
 * <p><b>Example:</b>
 *
 * <pre>{@code
 * // When you don't care about headers, use IgnoreHeaders with HasRecord
 * KafkaRecord<String, String> record = new KafkaRecord<>(headers, "key", "value");
 *
 * MatcherAssert.assertThat(
 *     record,
 *     new HasRecord<>(
 *         new IgnoreHeaders(),  // Ignore headers validation
 *         new HasKey<>("key"),
 *         new HasValue<>("value")
 *     )
 * );
 *
 * // Or use the simplified HasRecord(K, V) constructor which ignores headers
 * MatcherAssert.assertThat(
 *     record,
 *     new HasRecord<>("key", "value")
 * );
 * }</pre>
 */
public final class IgnoreHeaders extends TypeSafeMatcher<WithHeaders> {
    @Override
    protected boolean matchesSafely(final WithHeaders actual) {
        return true;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Ignored headers");
    }
}
