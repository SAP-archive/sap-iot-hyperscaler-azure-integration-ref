package com.sap.iot.azure.ref.ingestion.device.mapping;

import com.sap.iot.azure.ref.ingestion.model.device.mapping.DeviceMessage;

public class ConversionTestUtil {
    public static DeviceMessage getFaultyMessage() {
        return DeviceMessage.builder().deviceId("123").payload("[{\"asd\": 123; ERROR}]").build();
    }
}
