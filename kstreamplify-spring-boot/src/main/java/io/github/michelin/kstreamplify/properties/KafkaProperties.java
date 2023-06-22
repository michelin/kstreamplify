package io.github.michelin.kstreamplify.properties;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The Kafka properties class
 */
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {
    /**
     * The Kafka properties
     */
    private final Map<String, String> properties = new HashMap<>();

    /**
     * Return the Kafka properties as {@link java.util.Properties}
     * @return The Kafka properties
     */
    public Properties asProperties() {
        Properties props = new Properties();
        props.putAll(properties);
        return props;
    }
}
