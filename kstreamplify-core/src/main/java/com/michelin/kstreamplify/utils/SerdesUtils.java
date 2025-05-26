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
package com.michelin.kstreamplify.utils;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

/**
 * The Serdes utils class.
 *
 * @deprecated Use {@link com.michelin.kstreamplify.serde.SerdesUtils}.
 */
@Deprecated(since = "1.1.0")
public final class SerdesUtils {
    private SerdesUtils() {}

    /**
     * Return a key serdes for a requested class.
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdesForKey() {
        return com.michelin.kstreamplify.serde.SerdesUtils.getKeySerdes();
    }

    /**
     * Return a value serdes for a requested class.
     *
     * @param <T> The class of requested serdes
     * @return a serdes for requested class
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdesForValue() {
        return com.michelin.kstreamplify.serde.SerdesUtils.getValueSerdes();
    }
}
