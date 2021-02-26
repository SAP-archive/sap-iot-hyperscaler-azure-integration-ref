package com.sap.iot.azure.ref.integration.commons.constants;

import com.fasterxml.jackson.annotation.JsonValue;

public enum IngestionType {
    BATCHING("Batching"),
    STREAMING("Streaming");

    private final String value;

    IngestionType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
