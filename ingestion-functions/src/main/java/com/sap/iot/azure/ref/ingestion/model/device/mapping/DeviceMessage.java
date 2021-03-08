package com.sap.iot.azure.ref.ingestion.model.device.mapping;

import com.sap.iot.azure.ref.integration.commons.model.MessageEntity;
import com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DeviceMessage implements MessageEntity<SystemProperties> {
    private String payload;
    private String deviceId;
    private String enqueuedTime;

    // message source info
    private SystemProperties source;
}
