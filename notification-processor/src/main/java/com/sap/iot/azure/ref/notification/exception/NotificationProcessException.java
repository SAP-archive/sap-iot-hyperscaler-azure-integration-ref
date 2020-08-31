package com.sap.iot.azure.ref.notification.exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

public class NotificationProcessException extends IoTRuntimeException {
    public NotificationProcessException(String message, NotificationErrorType errorType, JsonNode identifier, boolean isTransient) {
        super(message, errorType, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }

    public NotificationProcessException(String message, Throwable cause, NotificationErrorType errorType, JsonNode identifier, boolean isTransient) {
        super(message, cause, errorType, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }
}
