package com.sap.iot.azure.ref.integration.commons.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * set of utility methods that helps to add identifier to an {@link com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException} to
 * identify the message / entity that's affected by the exception
 */
public class IdentifierUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static ObjectNode getIdentifier(String key, String value) {
        ObjectNode identifier = objectMapper.createObjectNode();
        identifier.put(key, value);

        return identifier;
    }

    public static ObjectNode getIdentifier(String key, String value, String key2, String value2) {
        ObjectNode identifier = getIdentifier(key, value);
        identifier.put(key2, value2);

        return identifier;
    }

    public static ObjectNode empty() {
        return objectMapper.createObjectNode();
    }
}
