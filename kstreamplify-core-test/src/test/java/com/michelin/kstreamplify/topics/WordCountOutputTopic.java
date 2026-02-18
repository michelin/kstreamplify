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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.cactoos.Scalar;

/** A {@link Scalar} that creates a {@link TestOutputTopic} for the word count application. */
public class WordCountOutputTopic implements Scalar<TestOutputTopic<String, Long>> {
    /** The topology test driver used to create the output topic. */
    private final TopologyTestDriver driver;

    /** @param driver The topology test driver used to create the output topic */
    public WordCountOutputTopic(final TopologyTestDriver driver) {
        this.driver = driver;
    }

    @Override
    public TestOutputTopic<String, Long> value() {
        return this.driver.createOutputTopic(
                "output-topic", Serdes.String().deserializer(), Serdes.Long().deserializer());
    }
}
