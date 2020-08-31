package com.sap.iot.azure.ref.integration.commons.exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

/**
 * any error that arises from lookup to SAP Mapping, Assignment & Model Configuration (Tags) API
 */
public class MappingLookupException extends IoTRuntimeException {
    public MappingLookupException(String message, JsonNode identifier, boolean isTransient) {
        this(message, CommonErrorType.MAPPING_LOOKUP_ERROR, identifier, isTransient);
    }

    public <T extends Enum<T> & ErrorType> MappingLookupException(String message, T errorType, JsonNode identifier, boolean isTransient) {
        super(message, errorType, InvocationContext.getContext().getInvocationId(),  identifier, isTransient);
    }
}
