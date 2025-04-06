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
package com.michelin.kstreamplify.serde;

import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

/** The Serde utils class. */
public final class SerdesUtils {
    private SerdesUtils() {}

    /**
     * Return a key serde for a requested class.
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getKeySerdes() {
        return getSerdes(true);
    }

    /**
     * Return a value serdes for a requested class.
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerdes() {
        return getSerdes(false);
    }

    /**
     * Return a serdes for a requested class.
     *
     * @param isSerdeForKey Is the serdes for a key or a value
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    private static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdes(boolean isSerdeForKey) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(KafkaStreamsExecutionContext.getSerdesConfig(), isSerdeForKey);
        return serde;
    }
}
