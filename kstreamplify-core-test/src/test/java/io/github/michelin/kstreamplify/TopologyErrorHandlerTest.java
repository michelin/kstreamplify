package io.github.michelin.kstreamplify;

import io.github.michelin.kstreamplify.avro.KafkaError;
import io.github.michelin.kstreamplify.error.TopologyErrorHandler;
import io.github.michelin.kstreamplify.error.ProcessingResult;
import io.github.michelin.kstreamplify.utils.SerdesUtils;
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

class TopologyErrorHandlerTest extends KafkaStreamsStarterTest {
    private final String AVRO_TOPIC = "avroTopic";
    private final String STRING_TOPIC = "stringTopic";
    private final String OUTPUT_AVRO_TOPIC = "outputAvroTopic";
    private final String OUTPUT_STRING_TOPIC = "outputStringTopic";

    @Override
    protected void topology(StreamsBuilder streamsBuilder) {
        KStream<String, ProcessingResult<String, String>> stringStream = streamsBuilder
                .stream(STRING_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> "error".equals(value) ?
                        ProcessingResult.fail(new NullPointerException(), value) : ProcessingResult.success(value));

        TopologyErrorHandler.catchErrors(stringStream)
                .to(OUTPUT_STRING_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, ProcessingResult<KafkaError, KafkaError>> avroStream = streamsBuilder
                .stream(AVRO_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.<KafkaError>getSerdesForValue()))
                .mapValues(value -> value == null ?
                        ProcessingResult.fail(new NullPointerException(), null) : ProcessingResult.success(value));

        TopologyErrorHandler.catchErrors(avroStream)
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

        inputTopic.pipeInput("key", "message");

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

        inputTopic.pipeInput("key", "error");

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

        inputTopic.pipeInput("key", avroModel);
        
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
        
        inputTopic.pipeInput("key", null);

        List<KafkaError> resultDlq = dlqTopic.readValuesToList();
        List<KafkaError> resultOutput = outputTopic.readValuesToList();

        assertEquals(1, resultDlq.size());
        assertEquals(0, resultOutput.size());
    }
}
