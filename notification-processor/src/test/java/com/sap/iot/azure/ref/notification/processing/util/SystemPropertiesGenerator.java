package com.sap.iot.azure.ref.notification.processing.util;

import com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties;
import org.joda.time.DateTime;

public class SystemPropertiesGenerator {
    public SystemProperties generateSystemProperties(){
        SystemProperties systemProperties = new SystemProperties();
        systemProperties.setPartitionKey("testPartitionKey");
        systemProperties.setPartitionId("testPartitionId");
        systemProperties.setOffset("testOffset");
        systemProperties.setEnqueuedTimeUtc(DateTime.now().toString());
        return systemProperties;
    }
}
