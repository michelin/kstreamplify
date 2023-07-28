/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.michelin.kstreamplify.converter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The class to convert Avro to Json
 */
public class AvroToJsonConverter {
    private AvroToJsonConverter() { }

    private static final Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    /**
     * Convert the record from avro format to json format
     * @param inputRecord the record in avro format
     * @return the record in json format
     */
    public static String convertRecord(GenericRecord inputRecord) {
        return gson.toJson(recordAsMap(inputRecord));
    }

    /**
     * convert avro to a map for json format
     * @param inputRecord record in avro
     * @return map for json format
     */
    private static Map<String, Object> recordAsMap(GenericRecord inputRecord) {
        Map<String, Object> recordMapping = new HashMap<>();

        for (Field field : inputRecord.getSchema().getFields()) {
            Object recordValue = inputRecord.get(field.name());

            if ((recordValue instanceof Utf8 || recordValue instanceof Instant)) {
                recordValue = recordValue.toString();
            }

            if (recordValue instanceof List<?> recordValueAsList) {
                recordValue = recordValueAsList
                        .stream()
                        .map(value -> {
                            if (value instanceof GenericRecord genericRecord) {
                                return recordAsMap(genericRecord);
                            } else {
                                return value.toString();
                            }
                        })
                        .toList();
            }

            if (recordValue instanceof Map<?, ?> recordValueAsMap) {
                Map<Object, Object> jsonMap = new HashMap<>();
                recordValueAsMap.forEach((key, value) -> {
                    if (value instanceof GenericRecord genericRecord) {
                        jsonMap.put(key, recordAsMap(genericRecord));
                    } else {
                        jsonMap.put(key, value.toString());
                    }
                });

                recordValue = jsonMap;
            }

            recordMapping.put(field.name(), recordValue);
        }

        return recordMapping;
    }
}
