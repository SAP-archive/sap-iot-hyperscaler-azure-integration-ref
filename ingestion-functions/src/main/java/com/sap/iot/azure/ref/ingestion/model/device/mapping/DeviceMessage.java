package com.sap.iot.azure.ref.ingestion.model.device.mapping;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DeviceMessage {
    private String payload;
    private String deviceId;
    private String enqueuedTime;
}
