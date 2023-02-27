package com.michelin.kafka.streams.starter.test.converter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JsonToAvroConverter {

    private static final String JSON_VALUE = "value";
    
    public static SpecificRecordBase jsonToAvro(String file, Schema schema) {
        return jsonToAvro((JsonObject) new JsonParser().parse(file), schema);
    }

    public static SpecificRecordBase jsonToAvro(JsonObject jsonEvent, Schema schema) {
        try {
            SpecificRecordBase record = baseClass(schema.getNamespace(), schema.getName()).getDeclaredConstructor().newInstance();
            populateGenericRecordFromJson(jsonEvent, record);
            return record;
        } catch (Exception e) {
            return null;
        }
    }

    private static void populateGenericRecordFromJson(JsonObject jsonObject, SpecificRecordBase record) {
        
            // Iterate over object attributes
            jsonObject.keySet().forEach(
                    currentKey -> {
                        try {
                            var currentValue = jsonObject.get(currentKey);

                        // If this is an object, add to prefix and call method again
                        if (currentValue instanceof JsonObject) {
                            Schema currentSchema = record.getSchema().getField(currentKey).schema();

                            // If the current value is a UNION
                            if (currentSchema.getType().equals(Schema.Type.UNION)) {
                                // Then research the first NOT NULL sub value
                                Optional<Schema> notNullSchema = currentSchema.getTypes().stream()
                                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                                        .findAny();

                                if (notNullSchema.isPresent()) {
                                    currentSchema = notNullSchema.get();
                                }
                            }

                            switch (currentSchema.getType()) {
                                case RECORD:
                                    SpecificRecordBase currentRecord = baseClass(record.getSchema().getNamespace(), currentSchema.getName()).getDeclaredConstructor().newInstance();
                                    populateGenericRecordFromJson((JsonObject) currentValue, currentRecord);
                                    record.put(currentKey, currentRecord);

                                    break;
                                case MAP:
                                    Map<String, Object> map = new HashMap<>();

                                    if (!currentSchema.getValueType().getType().equals(Schema.Type.RECORD)) {
                                        for (String key : ((JsonObject) currentValue).keySet()) {
                                            Object value = populateFieldWithCorrespondingType(((JsonObject) currentValue).get(key), currentSchema.getValueType().getType());
                                            map.put(key, value);
                                        }
                                    } else {
                                        for (String key : ((JsonObject) currentValue).keySet()) {
                                            SpecificRecordBase mapValueRecord = baseClass(record.getSchema().getNamespace(), currentSchema.getValueType().getName()).getDeclaredConstructor().newInstance();
                                            populateGenericRecordFromJson(((JsonObject) currentValue).get(key).getAsJsonObject(), mapValueRecord);
                                            map.put(key, mapValueRecord);
                                        }
                                    }

                                    record.put(currentKey, map);

                                    break;
                                default:
                                    record.put(currentKey,
                                            populateFieldWithCorrespondingType(currentValue, currentSchema.getType()));
                            }
                        }

                        // If this is an Array, call method for each one of them
                        else if (currentValue instanceof JsonArray) {
                            var arraySchema = record.getSchema().getField(currentKey).schema();
                            Schema arrayType = arraySchema.getType() != Schema.Type.UNION ?
                                    arraySchema :
                                    arraySchema.getTypes().stream()
                                            .filter(s -> s.getType() != Schema.Type.NULL)
                                            .findFirst().get();
                            Schema elementType = arrayType.getElementType();

                            if (elementType != null && Schema.Type.RECORD.equals(elementType.getType())) {
                                ArrayList<GenericRecord> recordArray = new ArrayList<>();
                                for (int i = 0; i < ((JsonArray) currentValue).size(); i++) {
                                    SpecificRecordBase currentRecord = baseClass(record.getSchema().getNamespace(), elementType.getName()).getDeclaredConstructor().newInstance();
                                    populateGenericRecordFromJson((JsonObject) ((JsonArray) currentValue).get(i), currentRecord);
                                    recordArray.add(currentRecord);
                                }
                                record.put(currentKey, recordArray);
                            } else {
                                ArrayList<Object> objArray = new ArrayList<>();
                                for (int i = 0; i < ((JsonArray) currentValue).size(); i++) {
                                    Object obj = populateFieldWithCorrespondingType((((JsonArray) currentValue).get(i)), elementType.getType());
                                    objArray.add(obj);
                                }
                                record.put(currentKey, objArray);
                            }
                        }
                        // Otherwise, put the value in the record after parsing according to its corresponding schema type
                        else {
                            if (!jsonObject.get(currentKey).isJsonNull()) {
                                populateFieldInRecordWithCorrespondingType(jsonObject, currentKey, record);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            );
    }

    private static Object populateFieldWithCorrespondingType(JsonElement jsonElement, Schema.Type type){
        switch (type) {
            case INT:
                return jsonElement.getAsInt();
            case LONG:
                return jsonElement.getAsLong();
            case FLOAT:
                return jsonElement.getAsFloat();
            case DOUBLE:
                return jsonElement.getAsDouble();
            case BOOLEAN:
                return jsonElement.getAsBoolean();
            default:
                return jsonElement.getAsString();
        }
    }

    private static void populateFieldInRecordWithCorrespondingType(JsonObject jsonObject, String fieldName, GenericRecord result) {
        Schema fieldSchema = result.getSchema().getField(fieldName).schema();
        Schema fieldType =
                fieldSchema.getType() != Schema.Type.UNION ?
                        fieldSchema :
                        fieldSchema.getTypes().stream()
                                .filter(s -> s.getType() != Schema.Type.NULL)
                                .findFirst().get();

        switch (fieldType.getType()) {
            case INT:
                result.put(fieldName, jsonObject.get(fieldName).getAsInt());
                break;
            case LONG:
                if (fieldType.getLogicalType() != null && fieldType.getLogicalType().getName().equals("timestamp-millis")) {
                    result.put(
                            fieldName,
                            Instant.ofEpochSecond(jsonObject.get(fieldName).getAsLong())
                    );
                } else {
                    result.put(fieldName, jsonObject.get(fieldName).getAsLong());
                }
                break;
            case FLOAT:
                result.put(fieldName, jsonObject.get(fieldName).getAsFloat());
                break;
            case DOUBLE:
                result.put(fieldName, jsonObject.get(fieldName).getAsDouble());
                break;
            case BOOLEAN:
                result.put(fieldName, jsonObject.get(fieldName).getAsBoolean());
                break;
            case BYTES:
                if (fieldType.getLogicalType() != null && fieldType.getLogicalType().getName().equals("decimal")) {
                    result.put(
                            fieldName,
                            new BigDecimal(jsonObject.get(fieldName).getAsString())
                                    .setScale(((LogicalTypes.Decimal) fieldType.getLogicalType()).getScale(), RoundingMode.HALF_UP)
                                    .round(new MathContext(((LogicalTypes.Decimal) fieldType.getLogicalType()).getPrecision()))
                    );
                } else {
                    // This is not supposed to happen, that would mean that the given field is in Byte format
                    result.put(fieldName, jsonObject.get(fieldName).getAsByte());
                }
                break;
            default:
                result.put(fieldName, jsonObject.get(fieldName).getAsString());
        }

    }

    @SuppressWarnings("unchecked")
    private static Class<SpecificRecordBase> baseClass(String baseNamespace, String typeName) {
        try {
            return (Class<SpecificRecordBase>) Class.forName(baseNamespace + "." + typeName);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
