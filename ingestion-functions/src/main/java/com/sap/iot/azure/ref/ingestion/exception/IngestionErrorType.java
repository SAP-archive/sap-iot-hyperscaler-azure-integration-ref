package com.sap.iot.azure.ref.ingestion.exception;

import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;

/**
 * error types that are specific to ingestion module
 */
public enum IngestionErrorType implements ErrorType {

    INVALID_DEVICE_MESSAGE("Invalid Device Message"),
    INVALID_PROCESSED_MESSAGE("Invalid Processed Message with no Avro Schema"),
    INVALID_TIMESTAMP("Invalid business time stamp");

    private final String description;

    IngestionErrorType(String description) {
        this.description = description;
    }

    @Override
    public String description() {
        return description;
    }
}
