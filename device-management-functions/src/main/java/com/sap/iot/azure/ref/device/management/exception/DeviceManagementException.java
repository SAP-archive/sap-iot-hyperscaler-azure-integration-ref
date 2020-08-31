package com.sap.iot.azure.ref.device.management.exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

public class DeviceManagementException extends IoTRuntimeException {

    public DeviceManagementException(String message, DeviceManagementErrorType errorType, JsonNode identifier, boolean isTransient) {
        super(message, errorType, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }

    public DeviceManagementException(String message, Throwable cause, DeviceManagementErrorType errorType, JsonNode identifier, boolean isTransient) {
        super(message, cause, errorType, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }
}
