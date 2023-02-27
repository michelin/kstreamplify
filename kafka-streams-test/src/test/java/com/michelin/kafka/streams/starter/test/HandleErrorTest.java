package com.michelin.kafka.streams.starter.test;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.michelin.kafka.streams.starter.avro.GenericError;
import com.michelin.kafka.streams.starter.commons.error.ErrorHandler;
import com.michelin.kafka.streams.starter.commons.error.ProcessingError;
import com.michelin.kafka.streams.starter.commons.utils.SerdesUtils;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class HandleErrorTest extends KafkaStreamsTestInitializer {
    @Override
    protected void applyTopology(StreamsBuilder builder) {
        // String case
        KStream<String, ProcessingError<String>> stringMapped = builder
                .stream("STRING", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(m -> new ProcessingError<>(new NullPointerException(), m));

        ErrorHandler.handleErrors(stringMapped);

        // Avro case
        var initialStreamAvro = builder
                .stream("AVRO", Consumed.with(Serdes.String(), SerdesUtils.getSerdesForValue()));

        KStream<String, ProcessingError<SpecificRecord>> avroMapped = initialStreamAvro
                .mapValues(m -> new ProcessingError<>(new NullPointerException(), m));

        ErrorHandler.handleErrors(avroMapped);
    }

    @Test
    void shouldSendExceptionToDLQForStringValues() {
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("STRING",
                new StringSerializer(), new StringSerializer());

        TestOutputTopic<String, GenericError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<GenericError>getSerdesForValue().deserializer());
        
        inputTopic.pipeInput("any", "any message");

        var result = dlqTopic.readValue();

        assertEquals("STRING", result.getTopic());
        assertNull(result.getCause());
        assertEquals(0, result.getOffset());
        assertEquals(0, result.getPartition());
        assertEquals("any message", result.getValue());
    }

    @Test
    void shouldSendExceptionToDLQForAvroValues() {
        TestInputTopic<String, GenericError> inputTopic = testDriver.createInputTopic("AVRO",
                new StringSerializer(), SerdesUtils.<GenericError>getSerdesForValue().serializer());

        TestOutputTopic<String, GenericError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<GenericError>getSerdesForValue().deserializer());

        var avroModel = GenericError.newBuilder()
                .setTopic("TOPIC")
                .setStack("STACK")
                .setPartition(1)
                .setOffset(2)
                .setCause("CAUSE")
                .setValue("A value")
                .build();

        inputTopic.pipeInput("any", avroModel);

        var result = dlqTopic.readValue();

        assertEquals("AVRO", result.getTopic());
        assertNull(result.getCause());
        assertEquals(0, result.getOffset());
        assertEquals(0, result.getPartition());

        // Test json value of avro original object
        var jsonObject = new Gson().fromJson(result.getValue(), JsonObject.class);
        assertEquals("STACK", jsonObject.get("stack").getAsString());
        assertEquals("CAUSE", jsonObject.get("cause").getAsString());
        assertEquals("TOPIC", jsonObject.get("topic").getAsString());
        assertEquals("A value", jsonObject.get("value").getAsString());
        assertEquals(1, jsonObject.get("partition").getAsInt());
        assertEquals(2, jsonObject.get("offset").getAsInt());
    }
}
