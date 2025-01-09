/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
     * @param host            The host
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
    public record StreamsHostInfo(String host, Integer port) {
    }
}
