package io.github.michelin.kstreamplify;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.github.michelin.kstreamplify.avro.KafkaTest;
import io.github.michelin.kstreamplify.avro.MapElement;
import io.github.michelin.kstreamplify.avro.SubKafkaTest;
import io.github.michelin.kstreamplify.avro.SubSubKafkaTest;
import io.github.michelin.kstreamplify.converter.AvroToJsonConverter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class AvroToJsonConverterTest {
    @Test
    void shouldConvertAvroToJson() {
        KafkaTest avro = getKafkaTest();

        String jsonString = AvroToJsonConverter.convertRecord(avro);

        var gson = new Gson();
        var jsonObject = gson.fromJson(jsonString, JsonObject.class);
        
        assertEquals("false", jsonObject.get("booleanField").getAsString());
        assertEquals("1970-01-01T00:00:00.001Z", jsonObject.get("dateField").getAsString());
        assertEquals("10", jsonObject.get("quantityField").getAsString());
        assertEquals("test", jsonObject.get("stringField").getAsString());

        assertEquals("1970-01-01T00:00:00.002Z", jsonObject.getAsJsonArray("split").get(0).getAsJsonObject().getAsJsonArray("subSplit").get(0).getAsJsonObject().get("subSubDateField").getAsString());
        assertEquals("1970-01-01T00:00:00.003Z", jsonObject.getAsJsonObject("members").getAsJsonObject("key1").get("mapDateField").getAsString());
        assertEquals("val1", jsonObject.getAsJsonObject("membersString").get("key1").getAsString());
        assertEquals("val1", jsonObject.getAsJsonArray("listString").get(0).getAsString());
        assertEquals("val2", jsonObject.getAsJsonArray("listString").get(1).getAsString());

        log.info(jsonString);
    }

    private KafkaTest getKafkaTest() {
        return KafkaTest.newBuilder()
                .setStringField("test")
                .setDateField(Instant.ofEpochMilli(1))
                .setQuantityField(BigDecimal.TEN)
                .setMembers(Map.of("key1", MapElement.newBuilder()
                        .setMapDateField(Instant.ofEpochMilli(3))
                        .setMapQuantityField(BigDecimal.ONE)
                        .build()))
                .setMembersString(Map.of("key1","val1"))
                .setListString(List.of("val1","val2"))
                .setSplit(List.of(
                        SubKafkaTest.newBuilder()
                                .setSubField("subTest")
                                .setSubSplit(List.of(
                                        SubSubKafkaTest.newBuilder()
                                                .setSubSubField("subSubTest")
                                                .setSubSubDateField(Instant.ofEpochMilli(2))
                                                .setSubSubIntField(8)
                                                .build()))
                                .build()))
                .build();
    }
}
