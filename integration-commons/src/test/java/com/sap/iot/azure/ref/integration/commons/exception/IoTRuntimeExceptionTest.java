package com.sap.iot.azure.ref.integration.commons.exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IoTRuntimeExceptionTest {
    @Test
    public void testJsonify() {
        String message = "message";
        CommonErrorType errorType = CommonErrorType.MAPPING_LOOKUP_ERROR;
        String invocationId = "invo";
        JsonNode identifier = IdentifierUtil.getIdentifier("Key1", "Value1");
        boolean isTransient = false;

        ObjectNode expected = new ObjectMapper().createObjectNode();
        expected.put("ErrorCode", CommonErrorType.MAPPING_LOOKUP_ERROR.name());
        expected.put("Description", CommonErrorType.MAPPING_LOOKUP_ERROR.description());
        expected.put("Message", message);
        expected.put("Identifier", identifier);
        expected.put("InvocationId", invocationId);
        expected.put("Transient", isTransient);

        IoTRuntimeException exception = new IoTRuntimeException(message, errorType, invocationId, identifier, isTransient);
        assertEquals(expected, exception.jsonify());
    }
}