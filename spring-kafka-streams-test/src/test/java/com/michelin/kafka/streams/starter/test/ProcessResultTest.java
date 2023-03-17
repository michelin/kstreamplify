package com.michelin.kafka.streams.starter.test;

import com.michelin.kafka.streams.starter.avro.KafkaError;
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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProcessResultTest extends KafkaStreamsTestTopology {
    private final String AVRO_TOPIC = "AVRO_TOPIC";
    private final String STRING_TOPIC = "STRING_TOPIC";
    private final String OUTPUT_AVRO_TOPIC = "OUTPUT_AVRO_TOPIC";
    private final String OUTPUT_STRING_TOPIC = "OUTPUT_STRING_TOPIC";

    @Override
    protected void buildTopology(StreamsBuilder streamsBuilder) {
        // String case
        KStream<String, ProcessingResult<String, String>> stringStream = streamsBuilder
                .stream(STRING_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> {
                    if("ERROR".equals(value)) {
                        return ProcessingResult.fail(new ProcessingError<>(new NullPointerException(), value));
                    }

                    return ProcessingResult.success(value);
                });

        ErrorHandler.catchErrors(stringStream)
                .to(OUTPUT_STRING_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Avro case
        KStream<String, ProcessingResult<KafkaError, KafkaError>> avroStream = streamsBuilder
                .stream(AVRO_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.<KafkaError>getSerdesForValue()))
                .mapValues(value -> {
                    if (value == null)  {
                        return ProcessingResult.fail(new ProcessingError<>(new NullPointerException(), null));
                    }

                    return ProcessingResult.success(value);
                });

        ErrorHandler.catchErrors(avroStream)
                .to(OUTPUT_AVRO_TOPIC, Produced.with(Serdes.String(),SerdesUtils.getSerdesForValue()));
    }

    @Test
    void shouldContinueWhenProcessingValueIsValid() {
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(STRING_TOPIC,
                new StringSerializer(), new StringSerializer());

        TestOutputTopic<String, KafkaError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<KafkaError>getSerdesForValue().deserializer());

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_STRING_TOPIC,
                new StringDeserializer(), new StringDeserializer());

        inputTopic.pipeInput("any", "any message");

        var resultDlq = dlqTopic.readValuesToList();
        var resultOutput = outputTopic.readValuesToList();

        assertEquals(0, resultDlq.size());
        assertEquals(1, resultOutput.size());
    }
    
    @Test
    void shouldSendExceptionToDLQWhenProcessingValueIsInvalid() {
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(STRING_TOPIC,
                new StringSerializer(), new StringSerializer());

        TestOutputTopic<String, KafkaError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<KafkaError>getSerdesForValue().deserializer());

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_STRING_TOPIC,
                new StringDeserializer(), new StringDeserializer());

        inputTopic.pipeInput("any", "ERROR");

        var resultDlq = dlqTopic.readValuesToList();
        var resultOutput = outputTopic.readValuesToList();

        assertEquals(1, resultDlq.size());
        assertEquals(0, resultOutput.size());
    }

    @Test
    void shouldContinueWhenProcessingValueIsValidAvro() {
        TestInputTopic<String, KafkaError> inputTopic = testDriver.createInputTopic(AVRO_TOPIC,
                new StringSerializer(), SerdesUtils.<KafkaError>getSerdesForValue().serializer());

        TestOutputTopic<String, KafkaError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<KafkaError>getSerdesForValue().deserializer());

        TestOutputTopic<String, KafkaError> outputTopic = testDriver.createOutputTopic(OUTPUT_AVRO_TOPIC,
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
        
        List<KafkaError> resultDlq = dlqTopic.readValuesToList();
        List<KafkaError> resultOutput = outputTopic.readValuesToList();

        assertEquals(0, resultDlq.size());
        assertEquals(1, resultOutput.size());
    }

    @Test
    void shouldContinueWhenProcessingValueIsInvalidAvro() {
        TestInputTopic<String, KafkaError> inputTopic = testDriver.createInputTopic(AVRO_TOPIC,
                new StringSerializer(), SerdesUtils.<KafkaError>getSerdesForValue().serializer());

        TestOutputTopic<String, KafkaError> dlqTopic = testDriver.createOutputTopic(DLQ_TOPIC,
                new StringDeserializer(), SerdesUtils.<KafkaError>getSerdesForValue().deserializer());

        TestOutputTopic<String, KafkaError> outputTopic = testDriver.createOutputTopic(OUTPUT_AVRO_TOPIC,
                new StringDeserializer(), SerdesUtils.<KafkaError>getSerdesForValue().deserializer());
        
        inputTopic.pipeInput("any", null);

        List<KafkaError> resultDlq = dlqTopic.readValuesToList();
        List<KafkaError> resultOutput = outputTopic.readValuesToList();

        assertEquals(1, resultDlq.size());
        assertEquals(0, resultOutput.size());
    }
}
