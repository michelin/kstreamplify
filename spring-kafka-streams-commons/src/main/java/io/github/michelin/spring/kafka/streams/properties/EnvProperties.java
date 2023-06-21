package io.github.michelin.spring.kafka.streams.properties;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class EnvProperties {

    @Getter
    private Properties properties;

    @Getter
    private Properties kafkaProperties;

    public EnvProperties() {
        this.properties = new Properties();
        this.kafkaProperties = new Properties();
    }

    public void loadProperties() {

        Yaml yaml = new Yaml();

        try (InputStream inputStream = EnvProperties.class.getClassLoader().getResourceAsStream("application.yml")) {

            Map<String, Object> allProps = yaml.load(inputStream);

            parseKey("", allProps);

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void parseKey(String key, Object map) {
        String sep = ".";
        if(StringUtils.isBlank(key)){
            sep = "";
        }
        if (map instanceof LinkedHashMap) {
            for (Object k : ((LinkedHashMap) map).keySet()) {
                parseKey(key + sep + k, ((LinkedHashMap) map).get(k));
            }
        } else {
            if (StringUtils.startsWith(key, "kafka.properties")) {
                kafkaProperties.put(
                        StringUtils.remove(key, "kafka.properties."),
                        map
                );
            } else
            properties.put(key, map);
        };
    }

}
