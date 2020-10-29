package com.sap.iot.azure.ref.integration.commons.exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

public class ADXClientException extends IoTRuntimeException {

    public ADXClientException(String message, JsonNode identifier, boolean isTransient) {
        super(message, CommonErrorType.ADX_ERROR, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }

    public ADXClientException(String message, Throwable cause, JsonNode identifier, boolean isTransient) {
        super(message, cause, CommonErrorType.ADX_ERROR, InvocationContext.getContext().getInvocationId(), identifier, isTransient);
    }
}
