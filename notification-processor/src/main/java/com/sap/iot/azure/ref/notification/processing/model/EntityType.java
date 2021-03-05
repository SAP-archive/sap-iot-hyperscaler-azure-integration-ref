package com.sap.iot.azure.ref.notification.processing.model;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum EntityType {
    ASSIGNMENT("com.sap.iot.i4c.Assignment"),
    STRUCTURE("com.sap.iot.i4c.Structure"),
    MAPPING("com.sap.iot.i4c.Mapping"),
    PROVIDERIOTMAPPING("com.sap.iot.i4c.ProviderIoTMapping"),
    SENSOR("com.sap.iot.i4c.Sensor"),
    STRUCTUREPROPERTY("com.sap.iot.i4c.StructureProperty"),
    SOURCEID("com.sap.iot.i4c.SourceId"),
    TAGS("com.sap.iot.i4c.Tags"),
    SENSORID("com.sap.iot.i4c.SensorId");

    private static final Map<String, EntityType> ENUM_LOOKUP = new HashMap<>();


    // Hashmap Implementation to populate lookup entries
    static {
        for (EntityType dataType : EnumSet.allOf(EntityType.class)) {
            ENUM_LOOKUP.put(dataType.name, dataType);
        }
    }

    private final String name;

    EntityType(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    public static EntityType ofType(String name) {
        return ENUM_LOOKUP.get(name);
    }

}
