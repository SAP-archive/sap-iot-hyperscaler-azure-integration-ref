package com.sap.iot.azure.ref.integration.commons.exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

public class TokenLookupException extends IoTRuntimeException {
    public TokenLookupException(String message, JsonNode identifier, boolean isTransient) {
        this(message, CommonErrorType.AUTH_TOKEN_LOOKUP_ERROR, identifier, isTransient);
    }

    public <T extends Enum<T> & ErrorType> TokenLookupException(String message, T errorType, JsonNode identifier, boolean isTransient) {
        super(message, errorType, InvocationContext.getContext().getInvocationId(),  identifier, isTransient);
    }
}
