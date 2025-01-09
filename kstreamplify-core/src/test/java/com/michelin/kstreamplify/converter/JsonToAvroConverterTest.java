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

package com.michelin.kstreamplify.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.michelin.kstreamplify.avro.EnumField;
import com.michelin.kstreamplify.avro.KafkaRecordStub;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class JsonToAvroConverterTest {

    private static final String JSON = "{"
        + "\"decimalField\":10.5,"
        + "\"intField\":123,"
        + "\"stringField\":\"test\","
        + "\"booleanField\":false,"
        + "\"uuidField\":\"dc306935-d720-427f-9ecd-ff87c0b15189\","
        + "\"timestampMillisField\":\"2024-03-27T19:51:01.815Z\","
        + "\"timestampMicrosField\":\"2024-03-27T19:51:01.815832Z\","
        + "\"localTimestampMillisField\":\"2024-03-27T20:51:01.815832\","
        + "\"localTimestampMicrosField\":\"2024-03-27T20:51:01.815832123\","
        + "\"timeMillisField\":\"20:51:01.815\","
        + "\"timeMicrosField\":\"20:51:01.815832\","
        + "\"enumField\":\"b\","
        + "\"dateField\":\"2024-03-27\","
        + "\"membersString\":{\"key1\":\"val1\",\"key2\":\"val2\"},"
        + "\"split\":[{"
        + "\"subSplit\":[{\"subSubIntField\":8,\"subSubField\":\"subSubTest\"}],"
        + "\"subField\":\"subTest\"}],"
        + "\"members\":{\"key1\":{\"mapQuantityField\":1}},"
        + "\"listString\":[\"val1\",\"val2\"]"
        + "}";

    @Test
    void shouldConvertJsonToObject() {
        assertEquals(Map.of("firstName", "John", "lastName", "Doe"),
            JsonToAvroConverter.jsonToObject("{\"firstName\":\"John\",\"lastName\":\"Doe\"}"));
    }

    @Test
    void shouldConvertJsonToObjectNull() {
        assertNull(JsonToAvroConverter.jsonToObject(null));
    }

    @Test
    void shouldConvertJsonToAvro() {
        KafkaRecordStub kafkaTest = (KafkaRecordStub) JsonToAvroConverter
            .jsonToAvro(JSON, KafkaRecordStub.getClassSchema());
        assertEquals("val1", kafkaTest.getMembersString().get("key1"));
        assertEquals(8, kafkaTest.getSplit().get(0).getSubSplit().get(0).getSubSubIntField());
        assertEquals("subSubTest", kafkaTest.getSplit().get(0).getSubSplit().get(0).getSubSubField());
        assertEquals("subTest", kafkaTest.getSplit().get(0).getSubField());
        assertFalse(kafkaTest.getBooleanField());
        assertEquals("1.0000", kafkaTest.getMembers().get("key1").getMapQuantityField().toString());
        assertEquals("10.5000", kafkaTest.getDecimalField().toString());
        assertEquals("123", String.valueOf(kafkaTest.getIntField()));
        assertEquals("test", kafkaTest.getStringField());
        assertEquals("val1", kafkaTest.getListString().get(0));
        assertEquals("val2", kafkaTest.getListString().get(1));
        assertEquals("2024-03-27", kafkaTest.getDateField().toString());
        assertEquals("20:51:01.815", kafkaTest.getTimeMillisField().toString());
        assertEquals("20:51:01.815832", kafkaTest.getTimeMicrosField().toString());
        assertEquals("2024-03-27T20:51:01.815832", kafkaTest.getLocalTimestampMillisField().toString());
        assertEquals("2024-03-27T20:51:01.815832123", kafkaTest.getLocalTimestampMicrosField().toString());
        assertEquals("2024-03-27T19:51:01.815Z", kafkaTest.getTimestampMillisField().toString());
        assertEquals("2024-03-27T19:51:01.815832Z", kafkaTest.getTimestampMicrosField().toString());
        assertEquals(EnumField.b, kafkaTest.getEnumField());
    }
}
