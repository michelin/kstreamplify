package com.michelin.kstreamplify.store;

import static com.michelin.kstreamplify.converter.AvroToJsonConverter.convertObject;
import static com.michelin.kstreamplify.converter.JsonToAvroConverter.jsonToObject;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * The state store record class.
 */
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StateStoreRecord {
    private String key;
    private Object value;
    private Long timestamp;

    /**
     * Constructor.
     *
     * @param key The key
     * @param value The value
     */
    public StateStoreRecord(String key, Object value) {
        this.key = key;
        // Convert the value to JSON then to object to avoid issue between Avro and Jackson
        this.value = jsonToObject(convertObject(value));
    }

    /**
     * Constructor.
     *
     * @param key The key
     * @param value The value
     * @param timestamp The timestamp
     */
    public StateStoreRecord(String key, Object value, Long timestamp) {
        this(key, value);
        this.timestamp = timestamp;
    }
}
