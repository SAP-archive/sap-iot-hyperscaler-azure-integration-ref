package com.sap.iot.azure.ref.ingestion.exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

public class IngestionRuntimeException extends IoTRuntimeException {

    public IngestionRuntimeException(String message, IngestionErrorType errorType, JsonNode identifier, boolean isTransient) {
        super(message, errorType, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }

    public IngestionRuntimeException(String message, Throwable cause, IngestionErrorType errorType, JsonNode identifier, boolean isTransient) {
        super(message, cause, errorType, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }
}