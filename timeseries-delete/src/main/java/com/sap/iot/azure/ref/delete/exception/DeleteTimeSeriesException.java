package com.sap.iot.azure.ref.delete.exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

public class DeleteTimeSeriesException extends IoTRuntimeException {
    public DeleteTimeSeriesException(String message, Throwable cause, DeleteTimeSeriesErrorType errorType, JsonNode identifier, boolean isTransient) {
        super(message, cause, errorType, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }

    public DeleteTimeSeriesException(String message, DeleteTimeSeriesErrorType errorType, JsonNode identifier, boolean isTransient) {
        super(message, errorType, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }
}
