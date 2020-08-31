package com.sap.iot.azure.ref.device.management.model;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public enum PropertyType {

    NUMERIC("Numeric"),
    NUMERIC_FLEXIBLE("NumericFlexible"),
    STRING("String"),
    BOOLEAN("Boolean"),
    JSON("JSON"),
    TIMESTAMP("Timestamp"),
    DATE_TIME("DateTime"),
    DATE("Date");

    private static final Map<String, PropertyType> ENUM_LOOKUP = new HashMap<>();

    // populate lookup entries
    static {
        for (PropertyType propertyType : EnumSet.allOf(PropertyType.class)) {
            ENUM_LOOKUP.put(propertyType.type, propertyType);
        }
    }

    private final String type;

    PropertyType(String type) {
        this.type =  type;
    }

    @JsonValue
    public String getType() {
        return type;
    }

    public static PropertyType ofValue(String type) {
        return ENUM_LOOKUP.get(type);
    }
}
