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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;

class GivenStageTest extends AbstractDslTest {
    @Test
    void shouldPipeSingleRecord() {
        test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .hasExactly(1)
                .containsRecord("123", "HELLO");
    }

    @Test
    void shouldPipeMultipleRecords() {
        test().given(INPUT)
                .record("1", "a")
                .record("2", "b")
                .when()
                .then(OUTPUT)
                .containsExactly(Map.entry("1", "A"), Map.entry("2", "B"));
    }

    @Test
    void shouldPipeBulkRecords() {
        test().given(INPUT)
                .records(List.of(KeyValue.pair("1", "a"), KeyValue.pair("2", "b")))
                .when()
                .then(OUTPUT)
                .containsExactly(Map.entry("1", "A"), Map.entry("2", "B"));
    }

    @Test
    void shouldPipeTimestampedRecord() {
        Instant timestamp = Instant.parse("2026-08-17T10:00:00Z");

        test().given(INPUT)
                .record("1", "a", timestamp)
                .when()
                .then(OUTPUT)
                .satisfies(record -> assertEquals(timestamp, record.getRecordTime()));
    }

    @Test
    void shouldFeedMultipleInputTopicsWithAnd() {
        test().given(INPUT)
                .record("1", "a")
                .and(INPUT_2)
                .record("2", "b")
                .when()
                .then(OUTPUT)
                .hasExactly(2)
                .containsKey("1")
                .containsKey("2");
    }

    @Test
    void shouldJoinTwoInputTopics() {
        test().given(USER_TOPIC)
                .record("1", "Anna")
                .and(ORDER_TOPIC)
                .record("1", "ORDER-1")
                .when()
                .then(JOIN_OUTPUT)
                .hasExactly(1)
                .containsRecord("1", "ORDER-1-Anna");
    }

    @Test
    void shouldAdvanceEventTimeBetweenRecords() {
        test().given(INPUT)
                .record("1", "a")
                .advanceTime(Duration.ofMinutes(5))
                .record("2", "b")
                .when()
                .then(OUTPUT)
                .satisfies(0, record -> assertEquals(INITIAL_TIME, record.getRecordTime()))
                .satisfies(1, record -> assertEquals(INITIAL_TIME.plus(Duration.ofMinutes(5)), record.getRecordTime()));
    }

    @Test
    void shouldAdvanceWallClockTimeWithoutAffectingEventTime() {
        test().given(INPUT)
                .advanceWallClockTime(Duration.ofMinutes(5))
                .record("1", "a")
                .when()
                .then(OUTPUT)
                .satisfies(record -> assertEquals(INITIAL_TIME, record.getRecordTime()));
    }

    @Test
    void shouldNavigateToAssertionsWithoutWhen() {
        test().given(INPUT).record("1", INVALID_VALUE).thenDlq().hasExactly(1);

        test().given(USER_TOPIC).record("1", "Anna").thenStateStore(USER_STORE).containsKey("1");

        test().given(INPUT).record("2", "b").then(OUTPUT).containsKey("2");
    }
}
