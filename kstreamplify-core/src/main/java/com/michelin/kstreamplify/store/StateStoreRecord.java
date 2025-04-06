/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kstreamplify.store;

import static com.michelin.kstreamplify.converter.AvroToJsonConverter.convertObject;
import static com.michelin.kstreamplify.converter.JsonToAvroConverter.jsonToObject;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

/** The state store record class. */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StateStoreRecord {
    private String key;
    private Object value;
    private Long timestamp;

    /** Constructor. */
    public StateStoreRecord() {
        // Default constructor
    }

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
