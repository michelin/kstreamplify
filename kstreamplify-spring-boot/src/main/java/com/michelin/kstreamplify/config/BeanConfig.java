package com.michelin.kstreamplify.config;

import com.michelin.kstreamplify.initializer.KafkaStreamsStarter;
import com.michelin.kstreamplify.initializer.SpringBootKafkaStreamsInitializer;
import com.michelin.kstreamplify.service.InteractiveQueriesService;
import com.michelin.kstreamplify.service.KubernetesService;
import com.michelin.kstreamplify.service.TopologyService;
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
    InteractiveQueriesService interactiveQueriesService(SpringBootKafkaStreamsInitializer initializer) {
        return new InteractiveQueriesService(initializer);
    }
}
