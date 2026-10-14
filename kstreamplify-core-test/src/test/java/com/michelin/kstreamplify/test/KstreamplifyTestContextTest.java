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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

class KstreamplifyTestContextTest extends AbstractDslTest {
    @Test
    void shouldReturnTheSameContextForTheWholeTest() {
        assertSame(test(), test());
    }

    @Test
    void shouldKeepEventTimeAdvancementAcrossTestCalls() {
        test().advanceTime(Duration.ofMinutes(5));

        test().given(INPUT).record("123", "hello");

        test().then(OUTPUT)
                .satisfies(record -> assertEquals(INITIAL_TIME.plus(Duration.ofMinutes(5)), record.getRecordTime()));
    }

    @Test
    void shouldAdvanceEventTimeFromTheContext() {
        test().advanceTime(Duration.ofMinutes(10))
                .given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .satisfies(record -> assertEquals(INITIAL_TIME.plus(Duration.ofMinutes(10)), record.getRecordTime()));
    }

    @Test
    void shouldUseInitialWallClockTimeAsDefaultEventTime() {
        test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .satisfies(record -> assertEquals(INITIAL_TIME, record.getRecordTime()));
    }

    @Test
    void shouldNotAdvanceEventTimeWhenAdvancingWallClockTime() {
        test().advanceWallClockTime(Duration.ofMinutes(30))
                .given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .satisfies(record -> assertEquals(INITIAL_TIME, record.getRecordTime()));
    }

    @Test
    void shouldTriggerWallClockPunctuatorWhenAdvancingWallClockTime() {
        test().when().then(TICK_OUTPUT).isEmpty();

        test().advanceWallClockTime(Duration.ofMinutes(2))
                .then(TICK_OUTPUT)
                .hasExactly(1)
                .containsRecord("tick", "tick");

        test().advanceWallClockTime(Duration.ofMinutes(2)).then(TICK_OUTPUT).hasExactly(2);
    }

    @Test
    void shouldMoveEventTimeToTheLastExplicitTimestamp() {
        Instant timestamp = Instant.parse("2026-08-17T10:00:00Z");

        test().given(INPUT)
                .record("1", "a", timestamp)
                .record("2", "b")
                .when()
                .then(OUTPUT)
                .satisfies(1, record -> assertEquals(timestamp, record.getRecordTime()));
    }

    @Test
    void shouldAccumulateOutputRecordsAcrossThenCalls() {
        test().given(INPUT).record("1", "a").when().then(OUTPUT).hasExactly(1);

        test().then(OUTPUT).hasExactly(1).containsKey("1");

        test().given(INPUT)
                .record("2", "b")
                .when()
                .then(OUTPUT)
                .hasExactly(2)
                .containsKey("1")
                .containsKey("2");
    }

    @Test
    void shouldAccumulateDlqRecordsAcrossThenDlqCalls() {
        test().given(INPUT).record("1", INVALID_VALUE).when().thenDlq().hasExactly(1);

        test().thenDlq().hasExactly(1).containsKey("1");
    }

    @Test
    void shouldFailWithDiagnosticsWhenStateStoreDoesNotExist() {
        AssertionFailedError error =
                assertThrows(AssertionFailedError.class, () -> test().thenStateStore("unknown-store"));

        assertTrue(error.getMessage().contains("No key-value state store named 'unknown-store'"));
        assertTrue(error.getMessage().contains(USER_STORE));
    }

    @Test
    void shouldExposeUnderlyingDriverAsEscapeHatch() {
        assertNotNull(test().driver());
        assertNotNull(test().given(INPUT).driver());
        assertNotNull(test().when().driver());
        assertNotNull(test().then(OUTPUT).driver());
        assertNotNull(test().thenDlq().driver());
        assertNotNull(test().thenStateStore(USER_STORE).driver());
    }
}
