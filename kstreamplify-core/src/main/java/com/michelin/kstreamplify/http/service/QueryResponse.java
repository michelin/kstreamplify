package com.michelin.kstreamplify.http.service;

import static com.michelin.kstreamplify.converter.AvroToJsonConverter.convertToJson;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRawValue;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * Rest key value.
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryResponse {
    @JsonRawValue
    private final String key;

    @JsonRawValue
    private final String value;

    private Long timestamp;

    private HostInfoResponse hostInfo;

    private List<PositionVector> positionVectors;

    /**
     * Constructor.
     *
     * @param key The key
     * @param value The value
     */
    public QueryResponse(Object key, Object value) {
        this.key = convertToJson(key);
        this.value = convertToJson(value);
    }

    /**
     * Constructor.
     *
     * @param key The key
     * @param value The value
     * @param timestamp The timestamp
     */
    public QueryResponse(Object key, Object value, Long timestamp, HostInfoResponse hostInfo,
                         List<PositionVector> positionVectors) {
        this.key = convertToJson(key);
        this.value = convertToJson(value);
        this.timestamp = timestamp;
        this.hostInfo = hostInfo;
        this.positionVectors = positionVectors;
    }

    /**
     * Query response position.
     */
    @Data
    @AllArgsConstructor
    public static class PositionVector {
        private String topic;
        private Integer partition;
        private Long offset;
    }
}