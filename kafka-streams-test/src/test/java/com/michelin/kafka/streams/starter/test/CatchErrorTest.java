package com.michelin.kafka.streams.starter.test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.michelin.kafka.streams.starter.avro.GenericError;
import com.michelin.kafka.streams.starter.test.error.ErrorHandler;
import com.michelin.kafka.streams.starter.test.error.ProcessingException;
import com.michelin.kafka.streams.starter.test.utils.SerdesUtils;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CatchErrorTest extends TopologyTestBase {

    @Override
    protected void applyTopology(StreamsBuilder builder) {
        // String case
        var initialStreamString = builder.stream("STRING", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Object> stringMapped = initialStreamString
                .mapValues(m -> new ProcessingException<>(new NullPointerException(), m));

        ErrorHandler.<String, String>catchError(stringMapped)
                .to("OUTPUT_STRING", Produced.with(Serdes.String(), Serdes.String()));

        // Avro case
        var initialStreamAvro = builder.stream("AVRO", Consumed.with(Serdes.String(), SerdesUtils.getSerdes()));
        KStream<String, Object> avroMapped = initialStreamAvro
                .mapValues(m -> new ProcessingException<>(new NullPointerException(), m));

        ErrorHandler.<String, SpecificRecord>catchError(avroMapped)
                .to("OUTPUT_AVRO", Produced.with(Serdes.String(), SerdesUtils.getSerdes()));
    }

    @Test
    void shouldCatchAllExceptionForStringValues() throws Exception {
       
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("STRING", new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, GenericError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC, new StringDeserializer(), SerdesUtils.<GenericError>getSerdes().deserializer());
        
        inputTopic.pipeInput("any", "any message");

        var result = dlqTopic.readValue();

        assertEquals("STRING", result.getTopic());
        assertNull(result.getCause());
        assertEquals(0, result.getOffset());
        assertEquals(0, result.getPartition());
        assertEquals("any message", result.getValue());

    }

    @Test
    void shouldCatchAllExceptionForAvroValues() {

        TestInputTopic<String, GenericError> inputTopic = testDriver.createInputTopic("AVRO", new StringSerializer(), SerdesUtils.<GenericError>getSerdes().serializer());
        TestOutputTopic<String, GenericError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC, new StringDeserializer(), SerdesUtils.<GenericError>getSerdes().deserializer());

        
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