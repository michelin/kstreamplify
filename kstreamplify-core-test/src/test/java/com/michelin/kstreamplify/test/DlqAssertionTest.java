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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

class DlqAssertionTest extends AbstractDslTest {
    @Test
    void shouldAssertDlqRecord() {
        test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .hasExactly(1)
                .containsKey("123")
                .containsError(IllegalStateException.class)
                .containsError(IllegalStateException.class, "Invalid value")
                .satisfies(record -> {
                    assertEquals("123", record.key());
                    assertEquals("java.lang.IllegalStateException", record.exceptionTypeName());
                    assertEquals("Invalid value", record.errorMessage());
                    assertEquals(INVALID_VALUE, record.error().getValue());
                    assertNotNull(record.contextMessage());
                    assertNotNull(record.headers());
                    assertNull(record.header("unknown-header"));
                })
                .containsHeader(CORRELATION_ID, "123")
                .contains(record -> "123".equals(record.key()))
                .containsErrorMessage("Invalid value")
                .hasExactly(1);
    }

    @Test
    void shouldAssertEmptyDlqWhenNoError() {
        test().given(INPUT).record("123", "hello").when().thenDlq().isEmpty();
    }

    @Test
    void shouldReturnUnmodifiableDlqRecords() {
        List<DlqRecord> records = test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .records();

        assertEquals(1, records.size());
        assertEquals("123", records.get(0).key());
        assertThrows(UnsupportedOperationException.class, () -> records.add(null));
    }

    @Test
    void shouldFailWhenDlqRecordCountDoesNotMatch() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .hasExactly(2));

        assertTrue(error.getMessage().contains("Expected 2 record(s) on DLQ topic 'DLQ_TOPIC' but found 1"));
        assertTrue(error.getMessage().contains("key='123'"));
        assertTrue(error.getMessage().contains("java.lang.IllegalStateException"));
        assertTrue(error.getMessage().contains("Invalid value"));
    }

    @Test
    void shouldFailWhenDlqIsNotEmpty() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .isEmpty());

        assertTrue(error.getMessage().contains("Expected no record on DLQ topic 'DLQ_TOPIC' but found 1"));
    }

    @Test
    void shouldFailWhenDlqKeyIsMissing() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .containsKey("456"));

        assertTrue(error.getMessage().contains("Expected a DLQ record for key '456'"));
        assertTrue(error.getMessage().contains("key='123'"));
    }

    @Test
    void shouldFailWithExpectedAndActualExceptionWhenErrorTypeDoesNotMatch() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .containsError(IllegalArgumentException.class));

        assertTrue(error.getMessage()
                .contains("Expected a DLQ record with exception type 'java.lang" + ".IllegalArgumentException'"));
        assertTrue(error.getMessage().contains("exception=java.lang.IllegalStateException"));
        assertTrue(error.getMessage().contains("message=Invalid value"));
    }

    @Test
    void shouldFailWhenErrorTypeAndMessageDoNotMatch() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .containsError(IllegalStateException.class, "Missing customer data"));

        assertTrue(error.getMessage().contains("and message containing 'Missing customer data'"));
        assertTrue(error.getMessage().contains("message=Invalid value"));
    }

    @Test
    void shouldFailWhenErrorMessageDoesNotMatch() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .containsErrorMessage("Missing customer data"));

        assertTrue(error.getMessage()
                .contains("Expected a DLQ record with an error message containing " + "'Missing customer data'"));
        assertTrue(error.getMessage().contains("message=Invalid value"));
    }

    @Test
    void shouldFailWhenDlqHeaderIsMissing() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .containsHeader(CORRELATION_ID, "456"));

        assertTrue(error.getMessage().contains("Expected a DLQ record with header 'correlation-id'='456'"));
    }

    @Test
    void shouldFailWhenNoDlqRecordMatchesPredicate() {
        AssertionFailedError error = assertThrows(AssertionFailedError.class, () -> test().given(INPUT)
                .record("123", INVALID_VALUE)
                .when()
                .thenDlq()
                .contains(record -> "456".equals(record.key())));

        assertTrue(error.getMessage().contains("Expected a DLQ record matching the given predicate"));
    }

    @Test
    void shouldFailWhenAssertedDlqRecordIndexDoesNotExist() {
        AssertionFailedError error = assertThrows(
                AssertionFailedError.class, () -> test().when().thenDlq().satisfies(record -> {}));

        assertTrue(error.getMessage().contains("Expected at least 1 record(s) on DLQ topic 'DLQ_TOPIC' but found 0"));
        assertTrue(error.getMessage().contains("<none>"));
    }
}
