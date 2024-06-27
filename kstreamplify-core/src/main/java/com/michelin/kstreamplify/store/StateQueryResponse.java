package com.michelin.kstreamplify.store;

import static com.michelin.kstreamplify.converter.JsonToAvroConverter.jsonToObject;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.michelin.kstreamplify.converter.AvroToJsonConverter;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * State query response.
 * This wraps the ${@link StateQueryData} but converts the key and value
 * to Object to avoid Avro serialization issues with Jackson.
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
public class StateQueryResponse {
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
    public StateQueryResponse(Object key, Object value) {
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
    public StateQueryResponse(Object key, Object value, Long timestamp, HostInfoResponse hostInfo,
                              List<PositionVector> positionVectors) {
        this(key, value);
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