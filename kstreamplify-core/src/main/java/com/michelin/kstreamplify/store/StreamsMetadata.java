package com.michelin.kstreamplify.store;

import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.state.HostInfo;

/**
 * State store metadata.
 */
@Getter
@NoArgsConstructor
public class StreamsMetadata {
    private Set<String> stateStoreNames;
    private StreamsHostInfo hostInfo;
    private Set<String> topicPartitions;

    /**
     * Constructor.
     *
     * @param stateStoreNames The state store names
     * @param host The host
     * @param topicPartitions The topic partitions
     */
    public StreamsMetadata(Set<String> stateStoreNames, HostInfo host, Set<TopicPartition> topicPartitions) {
        this.stateStoreNames = stateStoreNames;
        this.hostInfo = new StreamsHostInfo(host.host(), host.port());
        this.topicPartitions = topicPartitions
            .stream()
            .map(topicPartition -> topicPartition.topic() + "-" + topicPartition.partition())
            .collect(Collectors.toSet());
    }

    /**
     * State store host information.
     *
     * @param host The host
     * @param port The port
     */
    public record StreamsHostInfo(String host, Integer port) {}
}
