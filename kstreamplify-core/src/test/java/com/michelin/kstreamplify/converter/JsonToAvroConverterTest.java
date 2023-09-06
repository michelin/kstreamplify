package com.michelin.kstreamplify.converter;

import com.michelin.kstreamplify.avro.KafkaTestAvro;
import com.michelin.kstreamplify.converter.JsonToAvroConverter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class JsonToAvroConverterTest {

    private static final String JSON = "{\"membersString\":{\"key1\":\"val1\"},\"split\":[{\"subSplit\":[{\"subSubIntField\":8,\"subSubField\":\"subSubTest\"}],\"subField\":\"subTest\"}],\"booleanField\":false,\"members\":{\"key1\":{\"mapQuantityField\":1}},\"quantityField\":10,\"stringField\":\"test\",\"listString\":[\"val1\",\"val2\"]}";

    @Test
    void shouldConvertJsonToAvro() {
        KafkaTestAvro kafkaTest = (KafkaTestAvro) JsonToAvroConverter.jsonToAvro(JSON, KafkaTestAvro.getClassSchema());
        assertEquals("val1", kafkaTest.getMembersString().get("key1"));
        assertEquals(8, kafkaTest.getSplit().get(0).getSubSplit().get(0).getSubSubIntField());
        assertEquals("subSubTest", kafkaTest.getSplit().get(0).getSubSplit().get(0).getSubSubField());
        assertEquals("subTest", kafkaTest.getSplit().get(0).getSubField());
        assertEquals(false, kafkaTest.getBooleanField());
        assertEquals("1.0000", kafkaTest.getMembers().get("key1").getMapQuantityField().toString());
        assertEquals("10.0000", kafkaTest.getQuantityField().toString());
        assertEquals("test", kafkaTest.getStringField());
        assertEquals("val1", kafkaTest.getListString().get(0));
        assertEquals("val2", kafkaTest.getListString().get(1));
    }
}
