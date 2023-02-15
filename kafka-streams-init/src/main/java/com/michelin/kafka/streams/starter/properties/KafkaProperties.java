package com.michelin.kafka.streams.starter.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {
    private final Map<String, String> properties = new HashMap<>();

    public Properties asProperties() {
        Properties props = new Properties();
        props.putAll(this.properties);
        return props;
    }
}
