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
package com.michelin.kstreamplify.topics;

import static org.awaitility.Awaitility.await;

import com.michelin.kstreamplify.KafkaRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.pollinterval.PollInterval;
import org.cactoos.list.ListEnvelope;
import org.cactoos.scalar.Unchecked;

/**
 * A list of records polled from a consumer, waiting until the expected number of records is polled or the maximum
 * duration is reached.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public final class PolledRecords<K, V> extends ListEnvelope<KafkaRecord<K, V>> {
    /**
     * Primary ctor.
     *
     * @param consumer The consumer to poll from
     * @param atMost The maximum duration to wait for the expected records to be polled
     * @param pollInterval The interval between polls
     * @param expectedSize The expected number of records to be polled
     */
    public PolledRecords(
            final Consumer<K, V> consumer,
            final Duration atMost,
            final PollInterval pollInterval,
            final int expectedSize) {
        super(new Unchecked<>(() -> {
                    final List<KafkaRecord<K, V>> recordsList = new ArrayList<>();
                    await().atMost(atMost).pollInterval(pollInterval).until(() -> {
                        consumer.poll(Duration.ofMillis(500))
                                .forEach((final ConsumerRecord<K, V> record) ->
                                        recordsList.add(new KafkaRecord<>(record)));
                        return recordsList.size() >= expectedSize;
                    });
                    return recordsList;
                })
                .value());
    }
}
