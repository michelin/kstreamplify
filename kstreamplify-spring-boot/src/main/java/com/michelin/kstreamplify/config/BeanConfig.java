package com.michelin.kstreamplify.config;

import com.michelin.kstreamplify.http.service.KubernetesService;
import com.michelin.kstreamplify.http.service.StoreService;
import com.michelin.kstreamplify.http.service.TopologyService;
import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.initializer.SpringBootKafkaStreamsInitializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean configuration.
 */
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
     * Register the Store service as a bean.
     *
     * @param initializer The Kafka Streams initializer
     * @return The Store service
     */
    @Bean
    StoreService storeService(SpringBootKafkaStreamsInitializer initializer) {
        return new StoreService(initializer);
    }
}
