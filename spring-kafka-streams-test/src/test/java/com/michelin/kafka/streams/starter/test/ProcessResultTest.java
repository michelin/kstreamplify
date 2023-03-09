package com.michelin.kafka.streams.starter.test;

import com.michelin.kafka.streams.starter.avro.GenericError;
import com.michelin.kafka.streams.starter.commons.error.ErrorHandler;
import com.michelin.kafka.streams.starter.commons.error.ProcessingError;
import com.michelin.kafka.streams.starter.commons.error.ProcessingResult;
import com.michelin.kafka.streams.starter.commons.utils.SerdesUtils;
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

public class ProcessResultTest extends KafkaStreamsTestInitializer {
    @Override
    protected void applyTopology(StreamsBuilder builder) {
        // String case
        KStream<String, ProcessingResult<String,String>> stringMapped = builder
                .stream("STRING", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(m -> {
                    if("ERROR".equals(m)) {
                        return ProcessingResult.fail(new ProcessingError<>(new NullPointerException(), m));
                    }
                    return ProcessingResult.success(m);
                    
                });

        ErrorHandler.catchErrors(stringMapped)
                .to("OUTPUT_STRING", Produced.with(Serdes.String(), Serdes.String()));

        // Avro case
        KStream<String, ProcessingResult<GenericError, GenericError>> avroMapped = builder
                .stream("AVRO", Consumed.with(Serdes.String(), SerdesUtils.<GenericError>getSerdesForValue()))
                .mapValues(m -> {
                    if (m == null)  {
                        return ProcessingResult.fail(new ProcessingError<>(new NullPointerException(), m));
                    }
                    return ProcessingResult.success(m);
                });

        ErrorHandler.catchErrors(avroMapped)
                .to("OUTPUT_AVRO", Produced.with(Serdes.String(),SerdesUtils.getSerdesForValue()));
    }

    @Test
    void shouldContinueWhenProcessingValueIsValid() {
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("STRING",
                new StringSerializer(), new StringSerializer());

        TestOutputTopic<String, GenericError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<GenericError>getSerdesForValue().deserializer());

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("OUTPUT_STRING",
                new StringDeserializer(), new StringDeserializer());

        inputTopic.pipeInput("any", "any message");

        var resultDlq = dlqTopic.readValuesToList();
        var resultOutput = outputTopic.readValuesToList();

        assertEquals(0, resultDlq.size());
        assertEquals(1, resultOutput.size());
    }
    
    @Test
    void shouldSendExceptionToDLQWhenProcessingValueIsInvalid() {
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("STRING",
                new StringSerializer(), new StringSerializer());

        TestOutputTopic<String, GenericError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<GenericError>getSerdesForValue().deserializer());

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("OUTPUT_STRING",
                new StringDeserializer(), new StringDeserializer());

        inputTopic.pipeInput("any", "ERROR");
        var resultDlq = dlqTopic.readValuesToList();
        var resultOutput = outputTopic.readValuesToList();

        assertEquals(1, resultDlq.size());
        assertEquals(0, resultOutput.size());
    }

    @Test
    void shouldContinueWhenProcessingValueIsValidAvro() {
        TestInputTopic<String, GenericError> inputTopic = testDriver.createInputTopic("AVRO",
                new StringSerializer(), SerdesUtils.<GenericError>getSerdesForValue().serializer());

        TestOutputTopic<String, GenericError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<GenericError>getSerdesForValue().deserializer());

        TestOutputTopic<String, GenericError> outputTopic = testDriver.createOutputTopic("OUTPUT_AVRO",
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
        
        var resultDlq = dlqTopic.readValuesToList();
        var resultOutput = outputTopic.readValuesToList();

        assertEquals(0, resultDlq.size());
        assertEquals(1, resultOutput.size());
    }

    @Test
    void shouldContinueWhenProcessingValueIsInvalidAvro() {
        TestInputTopic<String, GenericError> inputTopic = testDriver.createInputTopic("AVRO",
                new StringSerializer(), SerdesUtils.<GenericError>getSerdesForValue().serializer());

        TestOutputTopic<String, GenericError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<GenericError>getSerdesForValue().deserializer())
                ;
        TestOutputTopic<String, GenericError> outputTopic = testDriver.createOutputTopic("OUTPUT_AVRO",
                new StringDeserializer(), SerdesUtils.<GenericError>getSerdesForValue().deserializer());
        
        inputTopic.pipeInput("any", null);
        
        var resultDlq = dlqTopic.readValuesToList();
        var resultOutput = outputTopic.readValuesToList();

        assertEquals(1, resultDlq.size());
        assertEquals(0, resultOutput.size());
    }
}
