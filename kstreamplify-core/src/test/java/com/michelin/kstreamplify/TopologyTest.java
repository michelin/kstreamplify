package com.michelin.kstreamplify;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarterTest;
import com.michelin.kstreamplify.model.TopologyExposeJsonModel;
import com.michelin.kstreamplify.services.ConvertTopology;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TopologyTest {

    @Test
    public void convertTopologyForRestTest() {
        KafkaStreamsInitializer kafkaStreamsInitializer = new KafkaStreamsInitializer();
        kafkaStreamsInitializer.init(new KafkaStreamsStarterTest());
        TopologyExposeJsonModel topology = ConvertTopology.convertTopologyForRest("STREAM", kafkaStreamsInitializer.getTopology());
        assertNotNull(topology);
    }
}
