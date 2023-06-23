package io.github.michelin.kstreamplify.properties;

import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Properties;

import static io.github.michelin.kstreamplify.constants.PropertyConstants.*;

public final class PropertiesUtils {

    public static Properties loadProperties() {
        Yaml yaml = new Yaml();

        try (InputStream inputStream = PropertiesUtils.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTY_FILE)) {

            LinkedHashMap<String, Object> propsMap = yaml.load(inputStream);

            return parsePropertiesMap(propsMap);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties loadKafkaProperties(Properties props) {
        Properties resultProperties = new Properties();
        for (var prop : props.entrySet()) {
            if (StringUtils.contains(prop.getKey().toString(), KAFKA_PROPERTIES_PREFIX)) {
                resultProperties.put(StringUtils.remove(prop.getKey().toString(), KAFKA_PROPERTIES_PREFIX + PROPERTY_SEPARATOR), prop.getValue());
            }
        }
        return resultProperties;
    }

    private static Properties parsePropertiesMap(LinkedHashMap<String, Object> map) {
        return parseKey("", map, null);
    }

    private static Properties parseKey(String key, Object map, Properties props) {
        if (props == null) {
            props = new Properties();
        }
        String sep = PROPERTY_SEPARATOR;
        if (StringUtils.isBlank(key)) {
            sep = "";
        }
        if (map instanceof LinkedHashMap) {
            for (Object k : ((LinkedHashMap<?, ?>) map).keySet()) {
                parseKey(key + sep + k, ((LinkedHashMap<?, ?>) map).get(k), props);
            }
        } else {
            props.put(key, map);
        }
        return props;
    }

}
