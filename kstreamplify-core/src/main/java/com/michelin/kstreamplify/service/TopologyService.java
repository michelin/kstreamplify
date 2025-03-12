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
package com.michelin.kstreamplify.service;

import com.michelin.kstreamplify.initializer.KafkaStreamsInitializer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Kafka Streams topology service. */
@Slf4j
@AllArgsConstructor
public class TopologyService {
    /** The topology path property. */
    public static final String TOPOLOGY_PATH_PROPERTY_NAME = "topology.path";

    /** The default topology path. */
    public static final String TOPOLOGY_DEFAULT_PATH = "topology";

    /** The Kafka Streams initializer. */
    private final KafkaStreamsInitializer kafkaStreamsInitializer;

    /**
     * Get the Kafka Streams topology.
     *
     * @return The Kafka Streams topology
     */
    public String getTopology() {
        return kafkaStreamsInitializer.getTopology().describe().toString();
    }
}
