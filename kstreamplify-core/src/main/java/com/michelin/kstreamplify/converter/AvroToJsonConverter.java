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
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

/**
 * The class to convert Avro to Json.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AvroToJsonConverter {
    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDate.class, new LocalDateTypeAdapter())
            .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeTypeAdapter())
            .registerTypeAdapter(LocalTime.class, new LocalTimeTypeAdapter())
            .setPrettyPrinting()
            .create();

    /**
     * Convert the record from avro format to json format.
     *
     * @param inputRecord the record in avro format
     * @return the record in json format
     */
    public static String convertRecord(GenericRecord inputRecord) {
        return gson.toJson(recordAsMap(inputRecord));
    }

    /**
     * Convert avro to a map for json format.
     *
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

            if (recordValue instanceof GenericRecord genericRecord) {
                recordValue = recordAsMap(genericRecord);
            }

            recordMapping.put(field.name(), recordValue);
        }

        return recordMapping;
    }

    private static class LocalDateTypeAdapter implements JsonSerializer<LocalDate>, JsonDeserializer<LocalDate> {

        private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        @Override
        public JsonElement serialize(final LocalDate date, final Type typeOfSrc,
                                     final JsonSerializationContext context) {
            return new JsonPrimitive(date.format(formatter));
        }

        @Override
        public LocalDate deserialize(JsonElement json, Type typeOfT,
                                     JsonDeserializationContext context) throws JsonParseException {
            return LocalDate.parse(json.getAsString(), formatter);
        }
    }

    private static class LocalDateTimeTypeAdapter implements JsonSerializer<LocalDateTime>,
            JsonDeserializer<LocalDateTime> {

        private static final DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        private static final DateTimeFormatter formatterNano =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");

        @Override
        public JsonElement serialize(LocalDateTime localDateTime, Type srcType,
                                     JsonSerializationContext context) {
            if (localDateTime.toString().length() == 29) {
                return new JsonPrimitive(formatterNano.format(localDateTime));
            }
            return new JsonPrimitive(formatter.format(localDateTime));
        }

        @Override
        public LocalDateTime deserialize(JsonElement json, Type typeOfT,
                                         JsonDeserializationContext context) throws JsonParseException {
            return LocalDateTime.parse(json.getAsString(), formatter);
        }
    }

    private static class LocalTimeTypeAdapter implements JsonSerializer<LocalTime>, JsonDeserializer<LocalTime> {

        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
        private static final DateTimeFormatter formatterNano = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");

        @Override
        public JsonElement serialize(LocalTime localTime, Type srcType,
                                     JsonSerializationContext context) {
            if (localTime.toString().length() == 15) {
                return new JsonPrimitive(formatterNano.format(localTime));
            }
            return new JsonPrimitive(formatter.format(localTime));
        }

        @Override
        public LocalTime deserialize(JsonElement json, Type typeOfT,
                                     JsonDeserializationContext context) throws JsonParseException {

            return LocalTime.parse(json.getAsString(), formatter);
        }
    }

}
