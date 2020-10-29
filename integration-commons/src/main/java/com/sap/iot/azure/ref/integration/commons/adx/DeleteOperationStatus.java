package com.sap.iot.azure.ref.integration.commons.adx;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum DeleteOperationStatus {
    SCHEDULED("Scheduled"),
    INPROGRESS("InProgress"),
    COMPLETED("Completed"),
    BADINPUT("BadInput"),
    FAILED("Failed");
    private static final Map<String, DeleteOperationStatus> ENUM_LOOKUP = new HashMap<>();

    static {
        for (DeleteOperationStatus deleteOperationStatus : EnumSet.allOf(DeleteOperationStatus.class)) {
            ENUM_LOOKUP.put(deleteOperationStatus.name, deleteOperationStatus);
        }
    }

    private final String name;

    DeleteOperationStatus(String name) {
        this.name = name;
    }

    public static DeleteOperationStatus ofType(String name) {
        return ENUM_LOOKUP.get(name);
    }

}
