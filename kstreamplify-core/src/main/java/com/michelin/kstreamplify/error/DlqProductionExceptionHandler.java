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

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

/** The class managing DLQ production exceptions. */
@Slf4j
public class DlqProductionExceptionHandler extends DlqExceptionHandler implements ProductionExceptionHandler {
    private static final Object GUARD = new Object();

    /** Constructor. */
    public DlqProductionExceptionHandler() {
        // Default constructor
    }

    /**
     * Constructor.
     *
     * @param producer A Kafka producer
     */
    public DlqProductionExceptionHandler(Producer<byte[], KafkaError> producer) {
        DlqExceptionHandler.producer = producer;
    }

    /**
     * Manage production exceptions.
     *
     * @param producerRecord The record to produce
     * @param productionException The exception on producing
     * @return FAIL or CONTINUE
     */
    @Override
    public ProductionExceptionHandlerResponse handle(
            ProducerRecord<byte[], byte[]> producerRecord, Exception productionException) {
        if (StringUtils.isBlank(KafkaStreamsExecutionContext.getDlqTopicName())) {
            log.warn("Failed to route production error to the designated DLQ (Dead Letter Queue) topic. "
                    + "Please make sure to define a DLQ topic in your KafkaStreamsStarter bean configuration.");
            return ProductionExceptionHandlerResponse.FAIL;
        }

        boolean retryable = productionException instanceof RetriableException;

        if (!retryable) {
            try {
                var builder = KafkaError.newBuilder();
                enrichWithException(builder, productionException, producerRecord.key(), producerRecord.value())
                        .setContextMessage("An exception occurred during the stream internal production")
                        .setOffset(-1)
                        .setPartition(producerRecord.partition() == null ? -1 : producerRecord.partition())
                        .setTopic(producerRecord.topic())
                        .setApplicationId(KafkaStreamsExecutionContext.getProperties()
                                .getProperty(StreamsConfig.APPLICATION_ID_CONFIG));

                producer.send(new ProducerRecord<>(
                                KafkaStreamsExecutionContext.getDlqTopicName(), producerRecord.key(), builder.build()))
                        .get();
            } catch (InterruptedException ie) {
                log.error(
                        "Interruption while sending the production exception {} for key {}, value {} "
                                + "and topic {} to DLQ topic {}",
                        productionException,
                        producerRecord.key(),
                        producerRecord.value(),
                        producerRecord.topic(),
                        KafkaStreamsExecutionContext.getDlqTopicName(),
                        ie);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error(
                        "Cannot send the production exception {} for key {}, value {} and topic {} to DLQ topic {}",
                        productionException,
                        producerRecord.key(),
                        producerRecord.value(),
                        producerRecord.topic(),
                        KafkaStreamsExecutionContext.getDlqTopicName(),
                        e);
                return ProductionExceptionHandlerResponse.CONTINUE;
            }

            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        return ProductionExceptionHandlerResponse.FAIL;
    }

    /** {@inheritDoc} */
    @Override
    public void configure(Map<String, ?> configs) {
        synchronized (GUARD) {
            if (producer == null) {
                instantiateProducer(DlqProductionExceptionHandler.class.getName(), configs);
            }
        }
    }
}
