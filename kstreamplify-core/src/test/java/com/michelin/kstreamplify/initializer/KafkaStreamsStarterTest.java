package com.michelin.kstreamplify.initializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.deduplication.DeduplicationUtils;
import com.michelin.kstreamplify.error.ProcessingResult;
import com.michelin.kstreamplify.error.TopologyErrorHandler;
import com.michelin.kstreamplify.utils.SerdesUtils;
import com.michelin.kstreamplify.utils.TopicWithSerde;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;

class KafkaStreamsStarterTest {

    @Test
    void shouldInstantiateKafkaStreamsStarter() {
        KafkaStreamsExecutionContext.registerProperties(new Properties());
        KafkaStreamsExecutionContext.setSerdesConfig(
            Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"));

        StreamsBuilder builder = new StreamsBuilder();
        KafkaStreamsStarterImpl starter = new KafkaStreamsStarterImpl();
        starter.topology(builder);

        assertNotNull(builder.build().describe());
        assertEquals("dlqTopicUnitTests", starter.dlqTopic());

        starter.onStart(null);
        assertTrue(starter.isStarted());
    }

    /**
     * Kafka Streams Starter implementation used for unit tests purpose.
     */
    @Getter
    static class KafkaStreamsStarterImpl extends KafkaStreamsStarter {
        private boolean started;

        @Override
        public void topology(StreamsBuilder streamsBuilder) {
            var streams = TopicWithSerdesTestHelper.inputTopicWithSerdes().stream(streamsBuilder);

            DeduplicationUtils.deduplicateKeys(streamsBuilder, streams,
                "deduplicateKeysStoreName", "deduplicateKeysRepartitionName",
                Duration.ZERO);
            DeduplicationUtils.deduplicateKeyValues(streamsBuilder, streams,
                "deduplicateKeyValuesStoreName",
                "deduplicateKeyValuesRepartitionName", Duration.ZERO);
            DeduplicationUtils.deduplicateWithPredicate(streamsBuilder, streams, Duration.ofMillis(1), null);

            var enrichedStreams = streams.mapValues(KafkaStreamsStarterImpl::enrichValue);
            var enrichedStreams2 = streams.mapValues(KafkaStreamsStarterImpl::enrichValue2);
            var processingResults = TopologyErrorHandler.catchErrors(enrichedStreams);
            TopologyErrorHandler.catchErrors(enrichedStreams2, true);
            TopicWithSerdesTestHelper.outputTopicWithSerdes().produce(processingResults);

        }

        @Override
        public String dlqTopic() {
            return "dlqTopicUnitTests";
        }

        @Override
        public void onStart(KafkaStreams kafkaStreams) {
            started = true;
        }

        private static ProcessingResult<String, String> enrichValue(KafkaError input) {
            if (input != null) {
                String output = "output field";
                return ProcessingResult.success(output);
            } else {
                return ProcessingResult.fail(new IOException("an exception occurred"), "output error");
            }
        }

        private static ProcessingResult<String, String> enrichValue2(KafkaError input) {
            if (input != null) {
                String output = "output field 2";
                return ProcessingResult.success(output);
            } else {
                return ProcessingResult.fail(new IOException("an exception occurred"), "output error 2");
            }
        }
    }

    /**
     * Topic with serdes helper used for unit tests purpose.
     *
     * @param <K> The key type
     * @param <V> The value type
     */
    public static class TopicWithSerdesTestHelper<K, V> extends TopicWithSerde<K, V> {
        private TopicWithSerdesTestHelper(String name, String appName, Serde<K> keySerde, Serde<V> valueSerde) {
            super(name, appName, keySerde, valueSerde);
        }

        public static TopicWithSerdesTestHelper<String, String> outputTopicWithSerdes() {
            return new TopicWithSerdesTestHelper<>("OUTPUT_TOPIC", "APP_NAME",
                Serdes.String(), Serdes.String());
        }

        public static TopicWithSerdesTestHelper<String, KafkaError> inputTopicWithSerdes() {
            return new TopicWithSerdesTestHelper<>("INPUT_TOPIC", "APP_NAME",
                Serdes.String(), SerdesUtils.getSerdesForValue());
        }
    }
}
