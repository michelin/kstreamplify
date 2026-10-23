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
package com.michelin.kstreamplify.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

class StateStoreAssertionTest extends AbstractDslTest {
    @Test
    void shouldAssertStateStoreContent() {
        test().given(USER_TOPIC)
                .record("USER-1", "active")
                .when()
                .thenStateStore(USER_STORE)
                .hasExactly(1)
                .containsKey("USER-1")
                .containsValue("active"::equals)
                .contains("USER-1", "active")
                .doesNotContainKey("USER-999")
                .hasExactly(1);
    }

    @Test
    void shouldAssertEmptyStateStore() {
        test().when().thenStateStore(USER_STORE).isEmpty().hasExactly(0);
    }

    @Test
    void shouldExposeUnderlyingStore() {
        assertEquals(
                "active",
                test().given(USER_TOPIC)
                        .record("USER-1", "active")
                        .when()
                        .<String, String>thenStateStore(USER_STORE)
                        .store()
                        .get("USER-1"));
    }

    @Test
    void shouldFailWhenKeyIsMissing() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(USER_TOPIC)
                .record("USER-1", "active")
                .when()
                .thenStateStore(USER_STORE)
                .containsKey("USER-999"));

        assertTrue(error.getMessage().contains("Expected state store 'user-store' to contain key 'USER-999'"));
        assertTrue(error.getMessage().contains("USER-1=active"));
    }

    @Test
    void shouldFailWhenKeyIsPresentButNotExpected() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(USER_TOPIC)
                .record("USER-1", "active")
                .when()
                .thenStateStore(USER_STORE)
                .doesNotContainKey("USER-1"));

        assertTrue(error.getMessage()
                .contains(
                        "Expected state store 'user-store' not to contain key 'USER-1' but it is mapped to 'active'"));
    }

    @Test
    void shouldFailWhenValueDoesNotMatch() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(USER_TOPIC)
                .record("USER-1", "active")
                .when()
                .thenStateStore(USER_STORE)
                .contains("USER-1", "inactive"));

        assertTrue(error.getMessage()
                .contains("Expected state store 'user-store' to map key 'USER-1' to 'inactive' but found 'active'"));
    }

    @Test
    void shouldFailWhenEntryCountDoesNotMatch() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(USER_TOPIC)
                .record("USER-1", "active")
                .when()
                .thenStateStore(USER_STORE)
                .hasExactly(2));

        assertTrue(error.getMessage().contains("Expected state store 'user-store' to contain 2 entries but found 1"));
        assertTrue(error.getMessage().contains("USER-1=active"));
    }

    @Test
    void shouldFailWhenStateStoreIsNotEmpty() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(USER_TOPIC)
                .record("USER-1", "active")
                .when()
                .thenStateStore(USER_STORE)
                .isEmpty());

        assertTrue(error.getMessage().contains("Expected state store 'user-store' to be empty but found 1 entries"));
    }

    @Test
    void shouldFailWhenNoValueMatchesPredicate() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(USER_TOPIC)
                .record("USER-1", "active")
                .when()
                .thenStateStore(USER_STORE)
                .containsValue("inactive"::equals));

        assertTrue(error.getMessage()
                .contains("Expected state store 'user-store' to contain a value matching the given predicate"));
    }
}
