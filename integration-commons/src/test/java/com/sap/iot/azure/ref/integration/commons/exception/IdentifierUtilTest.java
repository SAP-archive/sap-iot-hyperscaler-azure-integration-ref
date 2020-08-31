package com.sap.iot.azure.ref.integration.commons.exception;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class IdentifierUtilTest {
    @Test
    public void testOneEntry() {
        String sampleKey = "sampleKey";
        String sampleValue = "sampleValue";

        ObjectNode identifier = IdentifierUtil.getIdentifier(sampleKey, sampleValue);
        assertEquals(identifier.get(sampleKey).textValue(), sampleValue);
    }

    @Test
    public void testTwoEntries() {
        String sampleKey = "sampleKey";
        String sampleValue = "sampleValue";
        String sampleKey2 = "sampleKey2";
        String sampleValue2 = "sampleValue2";

        ObjectNode identifier = IdentifierUtil.getIdentifier(sampleKey, sampleValue, sampleKey2, sampleValue2);
        assertEquals(identifier.get(sampleKey).textValue(), sampleValue);
        assertEquals(identifier.get(sampleKey2).textValue(), sampleValue2);
    }

}