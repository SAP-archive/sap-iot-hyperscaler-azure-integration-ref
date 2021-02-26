package com.sap.iot.azure.ref.integration.commons.model.base.eventhub;

import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.IdentityHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SystemPropertiesTest {
    SystemProperties systemProperties = new SystemProperties();
    Map<String, Object> systemPropertiesMap = new IdentityHashMap<>();

    @Before
    public void init() {
        systemProperties.setPartitionKey("testPartitionKey");
        systemProperties.setPartitionId("testPartitionId");
        systemProperties.setOffset("testOffset");
        systemProperties.setEnqueuedTimeUtc(DateTime.now().toString());
        systemProperties.setSequenceNumber("testSequenceNumber");

        systemPropertiesMap.put(CommonConstants.ENQUEUED_TIME_UTC,
                systemProperties.getEnqueuedTimeUtc());
        systemPropertiesMap.put(CommonConstants.PARTITION_ID,
                systemProperties.getPartitionId());
        systemPropertiesMap.put(CommonConstants.PARTITION_KEY,
                systemProperties.getPartitionKey());
        systemPropertiesMap.put(CommonConstants.OFFSET,
                systemProperties.getOffset());
        systemPropertiesMap.put(CommonConstants.SEQUENCE_NUMBER,
                systemProperties.getSequenceNumber());
    }

    @Test
    public void testSystemProperties() {
        assertValidSystemProperties(systemProperties);
    }

    @Test
    public void testSystemPropertiesRemoveDuplicates() {
        systemPropertiesMap.put("Duplicate-Partition-Id",
                systemProperties.getPartitionId());
        systemPropertiesMap = SystemProperties.selectRelevantKeys(systemPropertiesMap);
        assertTrue(systemPropertiesMap.containsKey(CommonConstants.PARTITION_KEY));
        assertTrue(systemPropertiesMap.containsValue("testPartitionKey"));
        assertTrue(systemPropertiesMap.containsKey(CommonConstants.PARTITION_ID));
        assertTrue(systemPropertiesMap.containsValue("testPartitionId"));
        assertTrue(systemPropertiesMap.containsKey(CommonConstants.SEQUENCE_NUMBER));
        assertTrue(systemPropertiesMap.containsValue("testSequenceNumber"));
        assertTrue(systemPropertiesMap.containsKey(CommonConstants.OFFSET));
        assertTrue(systemPropertiesMap.containsValue("testOffset"));
        assertFalse(systemPropertiesMap.containsKey("Duplicate-Partition-Id"));
    }

    @Test
    public void testSystemPropertiesFrom() {
        SystemProperties systemPropertiesOutput = SystemProperties.from(systemPropertiesMap);
        assertValidSystemProperties(systemPropertiesOutput);
    }

    private void assertValidSystemProperties(SystemProperties systemProperties) {
        assertTrue(systemProperties.getPartitionKey().equals("testPartitionKey"));
        assertTrue(systemProperties.getOffset().equals("testOffset"));
        assertTrue(systemProperties.getPartitionId().equals("testPartitionId"));
        assertTrue(systemProperties.getSequenceNumber().equals("testSequenceNumber"));
    }
}