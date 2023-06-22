package io.github.michelin.spring.kafka.streams.properties;

import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public abstract class PropertiesUtils {

    public static Properties loadProperties() {
        Yaml yaml = new Yaml();

        try (InputStream inputStream = PropertiesUtils.class.getClassLoader().getResourceAsStream("application.yml")) {

            Map<String, Object> allProps = yaml.load(inputStream);

            return parseKey("", allProps);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties loadKafkaProperties(Properties props) {
        Properties resultProperties = new Properties();
        for (var prop : props.entrySet()) {
            if (StringUtils.contains(prop.getKey().toString(), "kafka.properties")) {
                resultProperties.put(StringUtils.remove(prop.getKey().toString(), "kafka.properties."), prop.getValue());
            }
        }
        return resultProperties;
    }

    private static Properties parseKey(String key, Object map) {
        Properties result = new Properties();
        String sep = ".";
        if (StringUtils.isBlank(key)) {
            sep = "";
        }
        if (map instanceof LinkedHashMap) {
            for (Object k : ((LinkedHashMap) map).keySet()) {
                parseKey(key + sep + k, ((LinkedHashMap) map).get(k));
            }
        } else {
            result.put(key, map);
        }
        return result;
    }

}
