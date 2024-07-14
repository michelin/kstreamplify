package com.michelin.kstreamplify.store;

import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * State query data.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
@Getter
@NoArgsConstructor
public class StateQueryData<K, V> {
    private K key;
    private V value;
    private Long timestamp;
    private HostInfoResponse hostInfo;
    private List<StateQueryResponse.PositionVector> positionVectors;

    /**
     * Constructor.
     *
     * @param key The key
     * @param value The value
     * @param timestamp The timestamp
     * @param hostInfo The host info
     * @param positionVectors The position vectors
     */
    public StateQueryData(K key, V value, Long timestamp, HostInfoResponse hostInfo,
                          List<StateQueryResponse.PositionVector> positionVectors) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.hostInfo = hostInfo;
        this.positionVectors = positionVectors;
    }

    /**
     * Convert to state query response.
     *
     * @param includeKey Include key in response
     * @param includeMetadata Include metadata in response
     * @return The state query response
     */
    public StateQueryResponse toStateQueryResponse(boolean includeKey, boolean includeMetadata) {
        if (includeMetadata) {
            return new StateQueryResponse(includeKey ? key : null, value, timestamp, hostInfo, positionVectors);
        }
        return new StateQueryResponse(includeKey ? key : null, value);
    }
}
