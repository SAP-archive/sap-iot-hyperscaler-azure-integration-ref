package com.sap.iot.azure.ref.ingestion.model.timeseries.raw.device.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class IoTDeviceModelMessage {
    private String sensorAlternateId;
    private String capabilityAlternateId;
    private List<IoTDeviceModelMeasure> measures;
}
