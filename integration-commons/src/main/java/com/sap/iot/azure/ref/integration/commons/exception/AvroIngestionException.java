package com.sap.iot.azure.ref.integration.commons.exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

/**
 * error in mapping ingestion data to Processed Time Series Avro format
 * this exception is always of permanent (non-transient) type
 */
public class AvroIngestionException extends IoTRuntimeException {
    public AvroIngestionException(String message, Throwable cause, JsonNode identifier) {
        super(message, cause, CommonErrorType.AVRO_EXCEPTION, InvocationContext.getContext().getInvocationId(), identifier, false);
    }

    public AvroIngestionException(String message, JsonNode identifier) {
        super(message, CommonErrorType.AVRO_EXCEPTION, InvocationContext.getContext().getInvocationId(), identifier, false);
    }
}
