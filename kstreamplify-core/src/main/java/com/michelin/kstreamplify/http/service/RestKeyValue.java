package com.michelin.kstreamplify.http.service;

import static com.michelin.kstreamplify.converter.AvroToJsonConverter.convertToJson;

import com.fasterxml.jackson.annotation.JsonRawValue;
import lombok.Getter;

/**
 * Rest key value.
 */
@Getter
public class RestKeyValue {
    @JsonRawValue
    private final String key;

    @JsonRawValue
    private final String value;

    public RestKeyValue(Object key, Object value) {
        this.key = convertToJson(key);
        this.value = convertToJson(value);
    }
}