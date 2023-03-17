package com.michelin.kafka.streams.starter.test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.michelin.kafka.streams.starter.avro.KafkaError;
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

class HandleErrorTest extends KafkaStreamsTestTopology {
    private final String AVRO_TOPIC = "AVRO_TOPIC";
    private final String STRING_TOPIC = "STRING_TOPIC";

    @Override
    protected void buildTopology(StreamsBuilder streamsBuilder) {
        // String case
        KStream<String, ProcessingError<String>> stringStream = streamsBuilder
                .stream(STRING_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> new ProcessingError<>(new NullPointerException(), value));

        ErrorHandler.handleErrors(stringStream);

        // Avro case
        KStream<String, ProcessingError<SpecificRecord>> avroStream = streamsBuilder
                .stream(AVRO_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getSerdesForValue()))
                .mapValues(value -> new ProcessingError<>(new NullPointerException(), value));

        ErrorHandler.handleErrors(avroStream);
    }

    @Test
    void shouldSendExceptionToDLQForStringValues() {
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(STRING_TOPIC,
                new StringSerializer(), new StringSerializer());

        TestOutputTopic<String, KafkaError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<KafkaError>getSerdesForValue().deserializer());
        
        inputTopic.pipeInput("any", "any message");

        KafkaError result = dlqTopic.readValue();

        assertEquals(STRING_TOPIC, result.getTopic());
        assertNull(result.getCause());
        assertEquals(0, result.getOffset());
        assertEquals(0, result.getPartition());
        assertEquals("any message", result.getValue());
    }

    @Test
    void shouldSendExceptionToDLQForAvroValues() {
        TestInputTopic<String, KafkaError> inputTopic = testDriver.createInputTopic(AVRO_TOPIC,
                new StringSerializer(), SerdesUtils.<KafkaError>getSerdesForValue().serializer());

        TestOutputTopic<String, KafkaError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<KafkaError>getSerdesForValue().deserializer());

        KafkaError avroModel = KafkaError.newBuilder()
                .setTopic("topic")
                .setStack("stack")
                .setPartition(0)
                .setOffset(0)
                .setCause("cause")
                .setValue("value")
                .build();

        inputTopic.pipeInput("any", avroModel);

        KafkaError result = dlqTopic.readValue();

        assertEquals(AVRO_TOPIC, result.getTopic());
        assertNull(result.getCause());
        assertEquals(0, result.getOffset());
        assertEquals(0, result.getPartition());

        JsonObject jsonObject = new Gson().fromJson(result.getValue(), JsonObject.class);
        assertEquals("stack", jsonObject.get("stack").getAsString());
        assertEquals("cause", jsonObject.get("cause").getAsString());
        assertEquals("topic", jsonObject.get("topic").getAsString());
        assertEquals("value", jsonObject.get("value").getAsString());
        assertEquals(0, jsonObject.get("partition").getAsInt());
        assertEquals(0, jsonObject.get("offset").getAsInt());
    }
}
