package com.sap.iot.azure.ref.device.management.model.cloudevents;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum SAPIoTCloudEventType {

    DEVICE_MANAGEMENT_CREATE_V1("com.sap.iot.abstraction.device.management.create.v1"),
    DEVICE_MANAGEMENT_UPDATE_V1("com.sap.iot.abstraction.device.management.update.v1"),
    DEVICE_MANAGEMENT_DELETE_V1("com.sap.iot.abstraction.device.management.delete.v1"),
    DEVICE_MANAGEMENT_CREATE_STATUS_V1("com.sap.iot.abstraction.device.management.create.status.v1"),
    DEVICE_MANAGEMENT_UPDATE_STATUS_V1("com.sap.iot.abstraction.device.management.update.status.v1"),
    DEVICE_MANAGEMENT_DELETE_STATUS_V1("com.sap.iot.abstraction.device.management.delete.status.v1");

    private static final Map<String, SAPIoTCloudEventType> ENUM_LOOKUP = new HashMap<>();

    // populate lookup entries
    static {
        for (SAPIoTCloudEventType dataType : EnumSet.allOf(SAPIoTCloudEventType.class)) {
            ENUM_LOOKUP.put(dataType.value, dataType);
        }
    }

    private final String value;

    SAPIoTCloudEventType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    public static SAPIoTCloudEventType ofValue(String name) {
        return ENUM_LOOKUP.get(name);
    }
}
