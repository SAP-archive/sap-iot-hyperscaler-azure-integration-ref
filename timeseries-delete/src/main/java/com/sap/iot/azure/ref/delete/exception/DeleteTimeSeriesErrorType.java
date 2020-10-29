package com.sap.iot.azure.ref.delete.exception;

import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;

public enum DeleteTimeSeriesErrorType implements ErrorType {
    STORAGE_QUEUE_ERROR("Storage Queue Error"),
    JSON_PROCESSING_ERROR("Json Processing Error"),
    INVALID_MESSAGE("Invalid Message"),
    RUNTIME_ERROR("Runtime Error"),
    URI_SYNTAX_ERROR("URI syntax error: Invalid Storage Queue Address"),
    ADX_DATA_QUERY_EXCEPTION("Data Service or data Client Exception");

    private final String description;

    DeleteTimeSeriesErrorType(String description) {
        this.description = description;
    }

    @Override
    public String description() {
        return description;
    }
}
