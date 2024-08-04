package com.michelin.kstreamplify.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.michelin.kstreamplify.avro.EnumField;
import com.michelin.kstreamplify.avro.KafkaRecordStub;
import com.michelin.kstreamplify.avro.MapElement;
import com.michelin.kstreamplify.avro.SubKafkaRecordStub;
import com.michelin.kstreamplify.avro.SubSubKafkaRecordStub;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class AvroToJsonConverterTest {
    @Test
    void shouldConvertObjectNull() {
        assertNull(AvroToJsonConverter.convertObject((Object) null));
    }

    @Test
    void shouldConvertObject() {
        String json = AvroToJsonConverter.convertObject(new PersonStub("John", "Doe"));
        assertEquals("""
            {
              "firstName": "John",
              "lastName": "Doe"
            }""", json);
    }

    @Test
    void shouldConvertGenericRecord() {
        String json = AvroToJsonConverter.convertRecord(buildKafkaRecordStub());
        assertEquals("""
            {
              "localTimestampMillisField": "2024-03-27T20:51:01.815832",
              "membersString": {
                "key1": "val1"
              },
              "decimalField": 10,
              "timeMillisField": "20:51:01.815",
              "booleanField": false,
              "dateField": "2024-03-27",
              "timestampMillisField": "2024-03-27T19:51:01.815Z",
              "intField": 5,
              "localTimestampMicrosField": "2024-03-27T20:51:01.815832123",
              "listString": [
                "val1",
                "val2"
              ],
              "timestampMicrosField": "2024-03-27T19:51:01.815832Z",
              "uuidField": "dc306935-d720-427f-9ecd-ff87c0b15189",
              "split": [
                {
                  "subSplit": [
                    {
                      "subSubIntField": 8,
                      "subSubDateField": "1970-01-01T00:00:00.002Z",
                      "subSubField": "subSubTest"
                    }
                  ],
                  "subField": "subTest"
                }
              ],
              "members": {
                "key1": {
                  "mapDateField": "1970-01-01T00:00:00.003Z",
                  "mapQuantityField": 1
                }
              },
              "timeMicrosField": "20:51:01.815832",
              "stringField": "test",
              "enumField": "b"
            }""", json);
    }

    @Test
    void shouldConvertListObject() {
        String json = AvroToJsonConverter.convertObject(List.of(new PersonStub("John", "Doe")));
        assertEquals("""
            [{
              "firstName": "John",
              "lastName": "Doe"
            }]""", json);
    }
    
    private KafkaRecordStub buildKafkaRecordStub() {
        return KafkaRecordStub.newBuilder()
                .setDecimalField(BigDecimal.TEN)
                .setIntField(5)
                .setStringField("test")
                .setBooleanField(false)
                .setUuidField(UUID.fromString("dc306935-d720-427f-9ecd-ff87c0b15189"))
                .setTimeMillisField(LocalTime.parse("20:51:01.815"))
                .setTimeMicrosField(LocalTime.parse("20:51:01.815832"))
                .setTimestampMillisField(Instant.parse("2024-03-27T19:51:01.815Z"))
                .setTimestampMicrosField(Instant.parse("2024-03-27T19:51:01.815832Z"))
                .setLocalTimestampMillisField(LocalDateTime.parse("2024-03-27T20:51:01.815832"))
                .setLocalTimestampMicrosField(LocalDateTime.parse("2024-03-27T20:51:01.815832123"))
                .setDateField(LocalDate.parse("2024-03-27"))
                .setEnumField(EnumField.b)
                .setMembers(Map.of("key1", MapElement.newBuilder()
                        .setMapDateField(Instant.ofEpochMilli(3))
                        .setMapQuantityField(BigDecimal.ONE)
                        .build()))
                .setMembersString(Map.of("key1", "val1"))
                .setListString(List.of("val1", "val2"))
                .setSplit(List.of(
                        SubKafkaRecordStub.newBuilder()
                                .setSubField("subTest")
                                .setSubSplit(List.of(
                                        SubSubKafkaRecordStub.newBuilder()
                                                .setSubSubField("subSubTest")
                                                .setSubSubDateField(Instant.ofEpochMilli(2))
                                                .setSubSubIntField(8)
                                                .build()))
                                .build()))
                .build();
    }

    record PersonStub(String firstName, String lastName) { }
}
