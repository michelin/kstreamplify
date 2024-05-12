package com.michelin.kstreamplify.model;

import static com.michelin.kstreamplify.converter.JsonToAvroConverter.jsonToObject;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.michelin.kstreamplify.converter.AvroToJsonConverter;
import com.michelin.kstreamplify.model.HostInfoResponse;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Rest key value.
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
public class QueryResponse {
    private Object key;
    private Object value;
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
        // Convert the Object to JSON then back to Object to avoid Avro serialization issues with Jackson
        this.key = jsonToObject(AvroToJsonConverter.convertObject(key));
        this.value = jsonToObject(AvroToJsonConverter.convertObject(value));
    }

    /**
     * Constructor.
     *
     * @param key The key
     * @param value The value
     * @param timestamp The timestamp
     * @param hostInfo The host info
     * @param positionVectors The position vectors
     */
    public QueryResponse(Object key, Object value, Long timestamp, HostInfoResponse hostInfo,
                         List<PositionVector> positionVectors) {
        // Convert the Object to JSON then back to Object to avoid Avro serialization issues with Jackson
        this.key = jsonToObject(AvroToJsonConverter.convertObject(key));
        this.value = jsonToObject(AvroToJsonConverter.convertObject(value));
        this.timestamp = timestamp;
        this.hostInfo = hostInfo;
        this.positionVectors = positionVectors;
    }

    /**
     * Position vector.
     *
     * @param topic The topic
     * @param partition The partition
     * @param offset The offset
     */
    public record PositionVector(String topic, Integer partition, Long offset) { }
}