package com.michelin.kstreamplify.initializer;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.deduplication.DeduplicationUtils;
import com.michelin.kstreamplify.error.ProcessingResult;
import com.michelin.kstreamplify.error.TopologyErrorHandler;
import com.michelin.kstreamplify.utils.TopicWithSerdesTest;
import org.apache.kafka.streams.StreamsBuilder;

import java.io.IOException;
import java.time.Duration;

public class KafkaStreamsStarterTest implements KafkaStreamsStarter{


    @Override
    public void topology(StreamsBuilder streamsBuilder) {

        var streams = TopicWithSerdesTest.inputTopicWithSerdes().stream(streamsBuilder);

        DeduplicationUtils.deduplicateKeys(streamsBuilder, streams, "deduplicateKeysStoreName", "deduplicateKeysRepartitionName", Duration.ZERO);
        DeduplicationUtils.deduplicateKeyValues(streamsBuilder, streams, "deduplicateKeyValuesStoreName", "deduplicateKeyValuesRepartitionName", Duration.ZERO);
        DeduplicationUtils.deduplicateWithPredicate(streamsBuilder, streams, Duration.ZERO, null);

        var enrichedStreams = streams.mapValues(KafkaStreamsStarterTest::enrichValue);
        var processingResults = TopologyErrorHandler.catchErrors(enrichedStreams);
        TopicWithSerdesTest.outputTopicWithSerdes().produce(processingResults);

    }

    private static ProcessingResult<String,String> enrichValue(KafkaError input) {
        if(input != null) {
            String output = "output field";
            return ProcessingResult.success(output);
        } else {
            return ProcessingResult.fail(new IOException("an exception occurred"), "output error");
        }
    }
}
