package com.michelin.kstreamplify.initializer;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.deduplication.DeduplicationUtils;
import com.michelin.kstreamplify.error.ProcessingResult;
import com.michelin.kstreamplify.error.TopologyErrorHandler;
import com.michelin.kstreamplify.utils.TopicWithSerdesTest;
import org.apache.kafka.streams.StreamsBuilder;

import java.io.IOException;
import java.time.Duration;

public class KafkaStreamsStarterTopologyTest extends KafkaStreamsStarter {


    @Override
    public void topology(StreamsBuilder streamsBuilder) {

        var streams = TopicWithSerdesTest.inputTopicWithSerdes().stream(streamsBuilder);

        DeduplicationUtils.deduplicateKeys(streamsBuilder, streams, "deduplicateKeysStoreName", "deduplicateKeysRepartitionName", Duration.ZERO);
        DeduplicationUtils.deduplicateKeyValues(streamsBuilder, streams, "deduplicateKeyValuesStoreName", "deduplicateKeyValuesRepartitionName", Duration.ZERO);
        DeduplicationUtils.deduplicateWithPredicate(streamsBuilder, streams, Duration.ofMillis(1), null);

        var enrichedStreams = streams.mapValues(KafkaStreamsStarterTopologyTest::enrichValue);
        var enrichedStreams2 = streams.mapValues(KafkaStreamsStarterTopologyTest::enrichValue2);
        var processingResults = TopologyErrorHandler.catchErrors(enrichedStreams);
        TopologyErrorHandler.catchErrors(enrichedStreams2, true);
        TopicWithSerdesTest.outputTopicWithSerdes().produce(processingResults);

    }

    @Override
    public String dlqTopic() {
        return "dlqTopicUnitTests";
    }

    private static ProcessingResult<String,String> enrichValue(KafkaError input) {
        if(input != null) {
            String output = "output field";
            return ProcessingResult.success(output);
        } else {
            return ProcessingResult.fail(new IOException("an exception occurred"), "output error");
        }
    }

    private static ProcessingResult<String,String> enrichValue2(KafkaError input) {
        if(input != null) {
            String output = "output field 2";
            return ProcessingResult.success(output);
        } else {
            return ProcessingResult.fail(new IOException("an exception occurred"), "output error 2");
        }
    }
}
