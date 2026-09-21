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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

class OutputAssertionTest extends AbstractDslTest {
    @Test
    void shouldAssertRecordCountAndContent() {
        test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .hasExactly(1)
                .containsKey("123")
                .doesNotContainKey("456")
                .containsRecord("123", "HELLO")
                .containsValue("HELLO"::equals)
                .containsHeader(CORRELATION_ID, "123")
                .containsExactly(Map.entry("123", "HELLO"))
                .allSatisfy(record -> assertEquals("HELLO", record.value()))
                .satisfies(record -> assertEquals("HELLO", record.value()))
                .hasExactly(1);
    }

    @Test
    void shouldAssertEmptyOutputWhenNoInput() {
        test().when().then(OUTPUT).isEmpty();
    }

    @Test
    void shouldReturnUnmodifiableRecords() {
        List<TestRecord<String, String>> records =
                test().given(INPUT).record("1", "a").when().then(OUTPUT).records();

        assertEquals(1, records.size());
        assertThrows(UnsupportedOperationException.class, () -> records.add(null));
    }

    @Test
    void shouldApplyAssertionToEveryRecord() {
        AtomicInteger counter = new AtomicInteger();

        test().given(INPUT)
                .record("1", "a")
                .record("2", "b")
                .when()
                .then(OUTPUT)
                .allSatisfy(record -> counter.incrementAndGet());

        assertEquals(2, counter.get());
    }

    @Test
    void shouldFailWhenRecordCountDoesNotMatch() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .hasExactly(2));

        assertTrue(error.getMessage().contains("Expected 2 record(s) on topic 'OUTPUT' but found 1"));
        assertTrue(error.getMessage().contains("123=HELLO"));
    }

    @Test
    void shouldFailWhenOutputIsNotEmpty() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .isEmpty());

        assertTrue(error.getMessage().contains("Expected no record on topic 'OUTPUT' but found 1"));
    }

    @Test
    void shouldFailWhenKeyIsMissing() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .containsKey("456"));

        assertTrue(error.getMessage().contains("Expected a record with key '456' on topic 'OUTPUT'"));
        assertTrue(error.getMessage().contains("123"));
    }

    @Test
    void shouldFailWhenKeyIsPresentButNotExpected() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .doesNotContainKey("123"));

        assertTrue(error.getMessage().contains("Expected no record with key '123' on topic 'OUTPUT'"));
    }

    @Test
    void shouldFailWhenRecordIsMissing() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .containsRecord("123", "WRONG"));

        assertTrue(error.getMessage().contains("Expected a record with key '123' and value 'WRONG'"));
        assertTrue(error.getMessage().contains("123=HELLO"));
    }

    @Test
    void shouldFailWhenNoValueMatchesPredicate() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .containsValue("WRONG"::equals));

        assertTrue(error.getMessage().contains("Expected a record matching the given value predicate"));
    }

    @Test
    void shouldFailWhenHeaderIsMissing() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", "hello")
                .when()
                .then(OUTPUT)
                .containsHeader(CORRELATION_ID, "456"));

        assertTrue(error.getMessage().contains("Expected a record with header 'correlation-id'='456'"));
        assertTrue(error.getMessage().contains("123 -> 123"));
    }

    @Test
    void shouldFailWithMissingAndUnexpectedKeysWhenExactCollectionSizeDiffers() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("1", "a")
                .when()
                .then(OUTPUT)
                .containsExactly(Map.entry("1", "A"), Map.entry("2", "B")));

        assertTrue(error.getMessage().contains("Missing keys:"));
        assertTrue(error.getMessage().contains("  2"));
        assertTrue(error.getMessage().contains("Unexpected keys:"));
    }

    @Test
    void shouldFailWhenExactCollectionDiffersAtIndex() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("1", "a")
                .record("2", "b")
                .when()
                .then(OUTPUT)
                .containsExactly(Map.entry("1", "A"), Map.entry("2", "WRONG")));

        assertTrue(error.getMessage().contains("Record at index 1 on topic 'OUTPUT' does not match"));
        assertTrue(error.getMessage().contains("Expected: 2=WRONG"));
        assertTrue(error.getMessage().contains("Actual:   2=B"));
    }

    @Test
    void shouldFailWhenAssertedRecordIndexDoesNotExist() {
        AssertionFailedError error = assertThrows(
                AssertionFailedError.class,
                () -> test().given(INPUT).record("1", "a").when().then(OUTPUT).satisfies(2, record -> {}));

        assertTrue(error.getMessage().contains("Expected at least 3 record(s) on topic 'OUTPUT' but found 1"));
    }

    @Test
    void shouldFailWhenApplyingAssertionToEveryRecordOfAnEmptyTopic() {
        AssertionFailedError error = assertThrows(
                AssertionFailedError.class, () -> test().when().then(OUTPUT).allSatisfy(record -> {}));

        assertTrue(error.getMessage().contains("Expected at least one record on topic 'OUTPUT' but found none"));
    }
}
