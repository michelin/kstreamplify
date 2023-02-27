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

package com.michelin.kafka.streams.starter.commons.converter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts Avro record to human readable Json record
 */
public class AvroToJsonConverter {
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Convert any Avro Model in an human readable Json text
     * MUST NOT be used for application interactions
     * @param inputRecord the avro model to convert
     * @return a human readable version of the avro
     */
    public static String convertRecord(GenericRecord inputRecord) {
        Map<String, Object> record = recordAsMap(inputRecord);
        return gson.toJson(record);
    }

    private static Map<String, Object> recordAsMap(GenericRecord inputRecord) {
        Map<String, Object> record = new HashMap<>();

        for (Field field : inputRecord.getSchema().getFields()) {
            Object col = inputRecord.get(field.name());
            if ((col instanceof Utf8 || col instanceof Instant)) {
                col = col.toString();
            }

            if (col instanceof List) {

                var list = (List<?>) col;
                if (list.size() == 0) {
                    continue;
                }

                var firstElement = list.get(0);
                if (firstElement instanceof GenericRecord) {
                    var listRecord = (List<GenericRecord>) col;
                    col = listRecord.stream().map(AvroToJsonConverter::recordAsMap).collect(Collectors.toList());
                } else {

                    col = list.stream().map(Object::toString).collect(Collectors.toList());
                }


            }

            if (col instanceof Map) {

                var map = (Map<?, ?>) col;
                if (map.size() == 0) {
                    continue;
                }

                var firstElement = map.values().toArray()[0];
                if (firstElement instanceof GenericRecord) {
                    var mapRecord = (Map<?, GenericRecord>) col;

                    var newMap = new HashMap<Object, Map<String, Object>>();
                    mapRecord.forEach((key, value) -> newMap.put(key, recordAsMap(value)));

                    col = newMap;
                } else {

                    var newMap = new HashMap<Object, String>();
                    map.forEach((key, value) -> newMap.put(key, value.toString()));

                    col = newMap;
                }
            }


            record.put(field.name(), col);
        }

        return record;
    }
}
