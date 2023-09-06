package com.michelin.kstreamplify.converter;

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


/**
 * The class to convert Json to Avro
 */
public class JsonToAvroConverter {
    /**
     * convert a file in json to avro
     * @param file the file in json
     * @param schema the avro schema to use
     * @return the record in avro
     */
    public static SpecificRecordBase jsonToAvro(String file, Schema schema) {
        return jsonToAvro((JsonObject) new JsonParser().parse(file), schema);
    }

    /**
     * convert json to avro
     * @param jsonEvent the json record
     * @param schema the avro schema to use
     * @return the record in avro
     */
    public static SpecificRecordBase jsonToAvro(JsonObject jsonEvent, Schema schema) {
        try {
            SpecificRecordBase record = baseClass(schema.getNamespace(), schema.getName()).getDeclaredConstructor().newInstance();
            populateGenericRecordFromJson(jsonEvent, record);
            return record;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * populate avro records from json
     * @param jsonObject json data to provide to the avro record
     * @param record the avro record to populate
     */
    private static void populateGenericRecordFromJson(JsonObject jsonObject, SpecificRecordBase record) {
            // Iterate over object attributes
            jsonObject.keySet().forEach(
                    currentKey -> {
                        try {
                            var currentValue = jsonObject.get(currentKey);

                        // If this is an object, add to prefix and call method again
                        if (currentValue instanceof JsonObject currentValueJsonObject) {
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
                                case RECORD -> {
                                    SpecificRecordBase currentRecord = baseClass(record.getSchema().getNamespace(), currentSchema.getName()).getDeclaredConstructor().newInstance();
                                    populateGenericRecordFromJson(currentValueJsonObject, currentRecord);
                                    record.put(currentKey, currentRecord);
                                }
                                case MAP -> {
                                    Map<String, Object> map = new HashMap<>();
                                    if (!currentSchema.getValueType().getType().equals(Schema.Type.RECORD)) {
                                        for (String key : currentValueJsonObject.keySet()) {
                                            Object value = populateFieldWithCorrespondingType(currentValueJsonObject.get(key), currentSchema.getValueType().getType());
                                            map.put(key, value);
                                        }
                                    } else {
                                        for (String key : currentValueJsonObject.keySet()) {
                                            SpecificRecordBase mapValueRecord = baseClass(record.getSchema().getNamespace(), currentSchema.getValueType().getName()).getDeclaredConstructor().newInstance();
                                            populateGenericRecordFromJson(currentValueJsonObject.get(key).getAsJsonObject(), mapValueRecord);
                                            map.put(key, mapValueRecord);
                                        }
                                    }
                                    record.put(currentKey, map);
                                }
                                default -> record.put(currentKey,
                                        populateFieldWithCorrespondingType(currentValue, currentSchema.getType()));
                            }
                        }

                        // If this is an Array, call method for each one of them
                        else if (currentValue instanceof JsonArray jsonArray) {
                            var arraySchema = record.getSchema().getField(currentKey).schema();
                            Schema arrayType = arraySchema.getType() != Schema.Type.UNION ?
                                    arraySchema :
                                    arraySchema.getTypes().stream()
                                            .filter(s -> s.getType() != Schema.Type.NULL)
                                            .findFirst().get();
                            Schema elementType = arrayType.getElementType();

                            if (elementType != null && Schema.Type.RECORD.equals(elementType.getType())) {
                                ArrayList<GenericRecord> recordArray = new ArrayList<>();
                                for (int i = 0; i < jsonArray.size(); i++) {
                                    SpecificRecordBase currentRecord = baseClass(record.getSchema().getNamespace(), elementType.getName()).getDeclaredConstructor().newInstance();
                                    populateGenericRecordFromJson((JsonObject) jsonArray.get(i), currentRecord);
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

    /**
     * populate field with corresponding type
     * @param jsonElement the json element to convert
     * @param type the type of the element
     * @return the element converted with the corresponding type
     */
    private static Object populateFieldWithCorrespondingType(JsonElement jsonElement, Schema.Type type){
        return switch (type) {
            case INT -> jsonElement.getAsInt();
            case LONG -> jsonElement.getAsLong();
            case FLOAT -> jsonElement.getAsFloat();
            case DOUBLE -> jsonElement.getAsDouble();
            case BOOLEAN -> jsonElement.getAsBoolean();
            default -> jsonElement.getAsString();
        };
    }

    /**
     * populate field in record with corresponding type
     * @param jsonObject data to provide to the avro record
     * @param fieldName the name to populate
     * @param result the avro record populated
     */
    private static void populateFieldInRecordWithCorrespondingType(JsonObject jsonObject, String fieldName, GenericRecord result) {
        Schema fieldSchema = result.getSchema().getField(fieldName).schema();
        Optional<Schema> optionalFieldType = fieldSchema.getType() != Schema.Type.UNION ? Optional.of(fieldSchema) :
                fieldSchema.getTypes()
                        .stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .findFirst();

        if (optionalFieldType.isPresent()) {
            Schema fieldType = optionalFieldType.get();
            switch (fieldType.getType()) {
                case INT -> result.put(fieldName, jsonObject.get(fieldName).getAsInt());
                case LONG -> {
                    if (fieldType.getLogicalType() != null && fieldType.getLogicalType().getName().equals("timestamp-millis")) {
                        result.put(fieldName, Instant.ofEpochSecond(jsonObject.get(fieldName).getAsLong()));
                    } else {
                        result.put(fieldName, jsonObject.get(fieldName).getAsLong());
                    }
                }
                case FLOAT -> result.put(fieldName, jsonObject.get(fieldName).getAsFloat());
                case DOUBLE -> result.put(fieldName, jsonObject.get(fieldName).getAsDouble());
                case BOOLEAN -> result.put(fieldName, jsonObject.get(fieldName).getAsBoolean());
                case BYTES -> {
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
                }
                default -> result.put(fieldName, jsonObject.get(fieldName).getAsString());
            }
        }
    }

    /**
     * get base class
     * @param baseNamespace the namespace of the class
     * @param typeName the class type
     * @return the base class
     */
    @SuppressWarnings("unchecked")
    private static Class<SpecificRecordBase> baseClass(String baseNamespace, String typeName) {
        try {
            return (Class<SpecificRecordBase>) Class.forName(baseNamespace + "." + typeName);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
