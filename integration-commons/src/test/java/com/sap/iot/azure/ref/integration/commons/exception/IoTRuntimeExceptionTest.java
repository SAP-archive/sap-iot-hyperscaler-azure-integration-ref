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

    @Test
    public void testGetIdentifier() {
        JsonNode identifier = IdentifierUtil.getIdentifier("key", "val");

        IoTRuntimeException exception = new IoTRuntimeException("message", CommonErrorType.MAPPING_LOOKUP_ERROR, "invocationId", identifier, false);
        assertEquals(identifier, exception.getIdentifiers());
    }

    @Test
    public void testGetWrappedIdentifier() {
        String key1 = "key1";
        String key2 = "key2";
        String val1 = "val1";
        String val2 = "val2";
        JsonNode identifier = IdentifierUtil.getIdentifier(key1, val1);
        JsonNode wrappedIdentifier = IdentifierUtil.getIdentifier(key2, val2);
        JsonNode expected = IdentifierUtil.getIdentifier(key1, val1, key2, val2);

        IoTRuntimeException wrappedException = new IoTRuntimeException("message", CommonErrorType.MAPPING_LOOKUP_ERROR, "invocationId", wrappedIdentifier, false);
        IoTRuntimeException exception = new IoTRuntimeException("message", wrappedException, CommonErrorType.MAPPING_LOOKUP_ERROR, "invocationId", identifier, false);
        assertEquals(expected, exception.getIdentifiers());
    }
}