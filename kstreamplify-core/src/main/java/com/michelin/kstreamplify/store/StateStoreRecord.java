package com.michelin.kstreamplify.store;

import static com.michelin.kstreamplify.converter.AvroToJsonConverter.convertObject;
import static com.michelin.kstreamplify.converter.JsonToAvroConverter.jsonToObject;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The state store record class.
 */
@Getter
@NoArgsConstructor
public class StateStoreRecord {
    private String key;
    private Object value;
    private Long timestamp;
    private StateStoreHostInfo hostInfo;

    /**
     * Constructor.
     *
     * @param key The key
     * @param value The value
     * @param timestamp The timestamp
     * @param hostInfo The host info
     */
    public StateStoreRecord(String key, Object value, Long timestamp, StateStoreHostInfo hostInfo) {
        this.key = key;
        this.value = jsonToObject(convertObject(value));
        this.timestamp = timestamp;
        this.hostInfo = hostInfo;
    }

    /**
     * Update the attributes depending on the requirements.
     *
     * @param includeKey Should include the key
     * @param includeMetadata Should include the metadata
     * @return The updated record
     */
    public StateStoreRecord updateAttributes(boolean includeKey, boolean includeMetadata) {
        if (!includeKey) {
            key = null;
        }

        if (!includeMetadata) {
            timestamp = null;
            hostInfo = null;
        }

        return this;
    }
}
