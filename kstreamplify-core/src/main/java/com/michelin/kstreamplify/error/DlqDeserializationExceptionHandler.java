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

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;

import com.michelin.kstreamplify.avro.KafkaError;
import com.michelin.kstreamplify.context.KafkaStreamsExecutionContext;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

/** The class managing deserialization exceptions. */
@Slf4j
public class DlqDeserializationExceptionHandler extends DlqExceptionHandler implements DeserializationExceptionHandler {
    private static final Object GUARD = new Object();
    private boolean handleSchemaRegistryRestException = false;
    private static final String DLQ_DESERIALIZATION_HANDLER_REST_CLIENT_EXCEPTION_ENABLED =
            "dlq.deserialization.handler.rest.client.exception.enabled";

    /** Constructor. */
    public DlqDeserializationExceptionHandler() {
        // Default constructor
    }

    /**
     * Constructor.
     *
     * @param producer A Kafka producer.
     */
    public DlqDeserializationExceptionHandler(Producer<byte[], KafkaError> producer) {
        DlqExceptionHandler.producer = producer;
    }

    /**
     * Manage deserialization exceptions.
     *
     * @param errorHandlerContext The error handler context
     * @param consumerRecord The record to deserialize
     * @param consumptionException The exception for the deserialization
     * @return FAIL or CONTINUE
     */
    @Override
    public DeserializationHandlerResponse handle(
            ErrorHandlerContext errorHandlerContext,
            ConsumerRecord<byte[], byte[]> consumerRecord,
            Exception consumptionException) {
        if (StringUtils.isBlank(KafkaStreamsExecutionContext.getDlqTopicName())) {
            log.warn("Failed to route deserialization error to the designated DLQ (Dead Letter Queue) topic. "
                    + "Please make sure to define a DLQ topic in your KafkaStreamsStarter bean configuration.");
            return DeserializationHandlerResponse.FAIL;
        }

        try {
            var builder = KafkaError.newBuilder();
            enrichWithException(builder, consumptionException, consumerRecord.key(), consumerRecord.value())
                    .setContextMessage("An exception occurred during the stream internal deserialization")
                    .setOffset(consumerRecord.offset())
                    .setPartition(consumerRecord.partition())
                    .setTopic(consumerRecord.topic())
                    .setApplicationId(
                            KafkaStreamsExecutionContext.getProperties().getProperty(APPLICATION_ID_CONFIG));

            boolean isCausedByKafka = consumptionException.getCause() instanceof KafkaException;
            boolean isRestClientSchemaRegistryException = consumptionException.getCause()
                    instanceof io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
            // If the cause of this exception is a KafkaException and if getCause == sourceException
            // (see Throwable.getCause - including SerializationException),
            // or the cause is a RestClientException from Schema Registry and the feature flag is enabled,
            // use to handle poison pill => sent message into dlq and continue our life.
            if (isCausedByKafka
                    || consumptionException instanceof org.apache.kafka.common.errors.SerializationException
                    || (isRestClientSchemaRegistryException && handleSchemaRegistryRestException)) {
                producer.send(new ProducerRecord<>(
                                KafkaStreamsExecutionContext.getDlqTopicName(), consumerRecord.key(), builder.build()))
                        .get();
                return DeserializationHandlerResponse.CONTINUE;
            }
        } catch (InterruptedException ie) {
            log.error(
                    "Interruption while sending the deserialization exception {} for key {}, "
                            + "value {} and topic {} to DLQ topic {}",
                    consumptionException,
                    consumerRecord.key(),
                    consumerRecord.value(),
                    consumerRecord.topic(),
                    KafkaStreamsExecutionContext.getDlqTopicName(),
                    ie);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error(
                    "Cannot send the deserialization exception {} for key {}, value {} and topic {} to DLQ topic {}",
                    consumptionException,
                    consumerRecord.key(),
                    consumerRecord.value(),
                    consumerRecord.topic(),
                    KafkaStreamsExecutionContext.getDlqTopicName(),
                    e);
        }

        // here we only have exception like UnknownHostException for example or TimeoutException ...
        // situation example:  we cannot ask schema registry because the url is unavailable
        return DeserializationHandlerResponse.FAIL;
    }

    /** {@inheritDoc} */
    @Override
    public void configure(Map<String, ?> configs) {
        synchronized (GUARD) {
            if (producer == null) {
                instantiateProducer(DlqDeserializationExceptionHandler.class.getName(), configs);
            }
            // Enable handling of Schema Registry RestClient exceptions in DLQ if the feature flag is set
            handleSchemaRegistryRestException = KafkaStreamsExecutionContext.isDlqFeatureEnabled(
                    DLQ_DESERIALIZATION_HANDLER_REST_CLIENT_EXCEPTION_ENABLED);
        }
    }
}
