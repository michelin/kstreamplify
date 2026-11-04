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
import org.junit.jupiter.api.Test;

class AssertionStageTest extends AbstractDslTest {
    @Test
    void shouldChainAssertionsOnSeveralOutputTopics() {
        test().given(INPUT)
                .record("1", "a")
                .and(USER_TOPIC)
                .record("1", "Anna")
                .and(ORDER_TOPIC)
                .record("1", "ORDER-1")
                .when()
                .then(OUTPUT)
                .containsRecord("1", "A")
                .and(JOIN_OUTPUT)
                .containsRecord("1", "ORDER-1-Anna");
    }

    @Test
    void shouldChainOutputDlqAndStateStoreAssertions() {
        test().given(INPUT)
                .record("1", "a")
                .record("2", INVALID_VALUE)
                .and(USER_TOPIC)
                .record("1", "Anna")
                .when()
                .then(OUTPUT)
                .hasExactly(1)
                .andDlq()
                .hasExactly(1)
                .containsKey("2")
                .andStateStore(USER_STORE)
                .containsKey("1");
    }

    @Test
    void shouldFeedMoreRecordsAfterAnAssertion() {
        test().given(INPUT)
                .record("1", "a")
                .when()
                .then(OUTPUT)
                .hasExactly(1)
                .andGiven(INPUT)
                .record("2", "b")
                .when()
                .then(OUTPUT)
                .hasExactly(2);
    }

    @Test
    void shouldAdvanceTimeFromTheWhenStage() {
        test().given(INPUT)
                .record("1", "a")
                .when()
                .advanceTime(Duration.ofMinutes(5))
                .advanceWallClockTime(Duration.ofSeconds(10))
                .then(OUTPUT)
                .andGiven(INPUT)
                .record("2", "b")
                .when()
                .then(OUTPUT)
                .satisfies(1, record -> assertEquals(INITIAL_TIME.plus(Duration.ofMinutes(5)), record.getRecordTime()));
    }
}
