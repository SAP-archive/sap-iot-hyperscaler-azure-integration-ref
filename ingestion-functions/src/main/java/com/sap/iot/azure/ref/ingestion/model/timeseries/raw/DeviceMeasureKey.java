package com.sap.iot.azure.ref.ingestion.model.timeseries.raw;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DeviceMeasureKey {
    private String sensorId;
    private String virtualCapabilityId;
}
