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
package com.michelin.kstreamplify.config;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.initializer.SpringBootKafkaStreamsInitializer;
import com.michelin.kstreamplify.service.KubernetesService;
import com.michelin.kstreamplify.service.TopologyService;
import com.michelin.kstreamplify.service.interactivequeries.keyvalue.KeyValueStoreService;
import com.michelin.kstreamplify.service.interactivequeries.keyvalue.TimestampedKeyValueStoreService;
import com.michelin.kstreamplify.service.interactivequeries.window.TimestampedWindowStoreService;
import com.michelin.kstreamplify.service.interactivequeries.window.WindowStoreService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Bean configuration. */
@Configuration
@ConditionalOnBean(KafkaStreamsStarter.class)
public class BeanConfig {
    /**
     * Register the Kubernetes service as a bean.
     *
     * @param initializer The Kafka Streams initializer
     * @return The Kubernetes service
     */
    @Bean
    KubernetesService kubernetesService(SpringBootKafkaStreamsInitializer initializer) {
        return new KubernetesService(initializer);
    }

    /**
     * Register the Topology service as a bean.
     *
     * @param initializer The Kafka Streams initializer
     * @return The Topology service
     */
    @Bean
    TopologyService topologyService(SpringBootKafkaStreamsInitializer initializer) {
        return new TopologyService(initializer);
    }

    /**
     * Register the key-value store service as a bean.
     *
     * @param initializer The Kafka Streams initializer
     * @return The key-value store service
     */
    @Bean
    KeyValueStoreService keyValueStoreService(SpringBootKafkaStreamsInitializer initializer) {
        return new KeyValueStoreService(initializer);
    }

    /**
     * Register the timestamped key-value store service as a bean.
     *
     * @param initializer The Kafka Streams initializer
     * @return The timestamped key-value store service
     */
    @Bean
    TimestampedKeyValueStoreService timestampedKeyValueStoreService(SpringBootKafkaStreamsInitializer initializer) {
        return new TimestampedKeyValueStoreService(initializer);
    }

    /**
     * Register the window store service as a bean.
     *
     * @param initializer The Kafka Streams initializer
     * @return The window store service
     */
    @Bean
    WindowStoreService windowStoreService(SpringBootKafkaStreamsInitializer initializer) {
        return new WindowStoreService(initializer);
    }

    /**
     * Register the timestamped window store service as a bean.
     *
     * @param initializer The Kafka Streams initializer
     * @return The timestamped window store service
     */
    @Bean
    TimestampedWindowStoreService timestampedWindowStoreService(SpringBootKafkaStreamsInitializer initializer) {
        return new TimestampedWindowStoreService(initializer);
    }
}
