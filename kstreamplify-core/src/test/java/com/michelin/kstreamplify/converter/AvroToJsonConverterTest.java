package com.michelin.kstreamplify.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.michelin.kstreamplify.avro.EnumField;
import com.michelin.kstreamplify.avro.KafkaTestAvro;
import com.michelin.kstreamplify.avro.MapElement;
import com.michelin.kstreamplify.avro.SubKafkaTestAvro;
import com.michelin.kstreamplify.avro.SubSubKafkaTestAvro;
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
    void shouldConvertAvroToJson() {
        KafkaTestAvro avro = getKafkaTest();

        String jsonString = AvroToJsonConverter.convertRecord(avro);

        var gson = new Gson();
        var jsonObject = gson.fromJson(jsonString, JsonObject.class);

        assertEquals("false", jsonObject.get("booleanField").getAsString());
        assertEquals("2024-03-27", jsonObject.get("dateField").getAsString());
        assertEquals("10", jsonObject.get("decimalField").getAsString());
        assertEquals("test", jsonObject.get("stringField").getAsString());
        assertEquals("20:51:01.815", jsonObject.get("timeMillisField").getAsString());
        assertEquals("20:51:01.815832", jsonObject.get("timeMicrosField").getAsString());
        assertEquals("2024-03-27T20:51:01.815832", jsonObject.get("localTimestampMillisField").getAsString());
        assertEquals("2024-03-27T20:51:01.815832123", jsonObject.get("localTimestampMicrosField").getAsString());
        assertEquals("2024-03-27T19:51:01.815Z", jsonObject.get("timestampMillisField").getAsString());
        assertEquals("2024-03-27T19:51:01.815832Z", jsonObject.get("timestampMicrosField").getAsString());
        assertEquals(EnumField.b.toString(), jsonObject.get("enumField").getAsString());
        assertEquals("1970-01-01T00:00:00.002Z",
                jsonObject.getAsJsonArray("split").get(0).getAsJsonObject().getAsJsonArray("subSplit")
                        .get(0).getAsJsonObject().get("subSubDateField").getAsString());
        assertEquals("1970-01-01T00:00:00.003Z",
                jsonObject.getAsJsonObject("members").getAsJsonObject("key1").get("mapDateField")
                        .getAsString());
        assertEquals("val1", jsonObject.getAsJsonObject("membersString").get("key1").getAsString());
        assertEquals("val1", jsonObject.getAsJsonArray("listString").get(0).getAsString());
        assertEquals("val2", jsonObject.getAsJsonArray("listString").get(1).getAsString());

        log.info(jsonString);
    }


    private KafkaTestAvro getKafkaTest() {
        return KafkaTestAvro.newBuilder()
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
                        SubKafkaTestAvro.newBuilder()
                                .setSubField("subTest")
                                .setSubSplit(List.of(
                                        SubSubKafkaTestAvro.newBuilder()
                                                .setSubSubField("subSubTest")
                                                .setSubSubDateField(Instant.ofEpochMilli(2))
                                                .setSubSubIntField(8)
                                                .build()))
                                .build()))
                .build();
    }
}
