package io.github.michelin.spring.kafka.streams.error;

import io.github.michelin.spring.kafka.streams.avro.KafkaError;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

@Slf4j
public abstract class DlqExceptionHandler {
    protected KafkaProducer<byte[], KafkaError> producer;

    protected void createProducer(Map<String, ?> configs) {
        Properties properties = new Properties();
        properties.putAll(configs);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, getClass().getSimpleName());
        producer = new KafkaProducer<>(properties);
    }

    protected void handleException(String key, String value, String topic, Exception e, Exception se) {
        log.error("Cannot write the production exception into DLQ", e);
        log.error("Source exception: ", se);
        log.error("Key: {}", key);
        log.error("Value: {}", value);
        log.error("Target topic: {}", topic);
    }

    protected KafkaError.Builder enrichWithException(KafkaError.Builder builder, Exception exception, byte[] key, byte[] value) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exception.printStackTrace(pw);
        log.debug("Handling " + getClass().getSimpleName() + "exception.", exception);

        boolean tooLarge = exception instanceof RecordTooLargeException;

        builder.setCause(exception.getCause() != null ? exception.getCause().getMessage() : "Unknown cause")
                .setValue(tooLarge ? "Value is too large. Setting key in ByteValue instead of value" : null)
                .setStack(sw.toString())
                .setByteValue(tooLarge ? ByteBuffer.wrap(key) : ByteBuffer.wrap(value));

        return builder;
    }


}
