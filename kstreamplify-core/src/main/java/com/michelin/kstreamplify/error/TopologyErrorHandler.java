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
package com.michelin.kstreamplify.error;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import com.michelin.kstreamplify.serde.SerdesUtils;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/** The topology error handler class. */
@Slf4j
public class TopologyErrorHandler {
    private static final String BRANCHING_NAME_NOMINAL = "branch-nominal";

    private TopologyErrorHandler() {}

    /**
     * Catch the errors from the given stream.
     *
     * @param stream The stream of processing result that may contain processing errors
     * @param <K> The key type
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     * @return A stream filtered from all processing errors
     */
    public static <K, V, V2> KStream<K, V> catchErrors(KStream<K, ProcessingResult<V, V2>> stream) {
        return catchErrors(stream, false);
    }

    /**
     * Catch the errors from the given stream.
     *
     * @param stream The stream of processing result that may contain processing errors
     * @param allowTombstone Allow sending tombstone in DLQ topic or to be returned
     * @param <K> The key type
     * @param <V> The type of the successful record
     * @param <V2> The type of the failed record
     * @return A stream filtered from all processing errors
     */
    public static <K, V, V2> KStream<K, V> catchErrors(
            KStream<K, ProcessingResult<V, V2>> stream, boolean allowTombstone) {
        Map<String, KStream<K, ProcessingResult<V, V2>>> branches;

        String branchNamePrefix = stream.toString().split("@")[1];
        if (!allowTombstone) {
            branches = stream.filter((key, value) -> value != null)
                    .filterNot((key, value) -> value.getValue() == null && value.getError() == null)
                    .split(Named.as(branchNamePrefix))
                    .branch((key, value) -> value.isValid(), Branched.as(BRANCHING_NAME_NOMINAL))
                    .defaultBranch(Branched.withConsumer(
                            ks -> TopologyErrorHandler.handleErrors(ks.mapValues(ProcessingResult::getError))));
        } else {
            branches = stream.filter((key, value) -> value != null)
                    .split(Named.as(branchNamePrefix))
                    .branch((key, value) -> value.getError() == null, Branched.as(BRANCHING_NAME_NOMINAL))
                    .defaultBranch(Branched.withConsumer(
                            ks -> TopologyErrorHandler.handleErrors(ks.mapValues(ProcessingResult::getError))));
        }

        return branches.get(branchNamePrefix + BRANCHING_NAME_NOMINAL).mapValues(ProcessingResult::getValue);
    }

    /**
     * Process a stream of processing errors and route it to the configured DLQ topic.
     *
     * @param errorsStream The stream of processing errors
     * @param <K> The key type
     * @param <V> The value type
     */
    private static <K, V> void handleErrors(KStream<K, ProcessingError<V>> errorsStream) {
        if (StringUtils.isBlank(KafkaStreamsExecutionContext.getDlqTopicName())) {
            log.warn("Failed to route topology error to the designated DLQ (Dead Letter Queue) topic. "
                    + "Please make sure to define a DLQ topic in your KafkaStreamsStarter bean configuration.");
            return;
        }
        errorsStream
                .map((key, value) -> new KeyValue<>(key == null ? "null" : key.toString(), value))
                .processValues(GenericErrorProcessor<V>::new)
                .to(
                        KafkaStreamsExecutionContext.getDlqTopicName(),
                        Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
