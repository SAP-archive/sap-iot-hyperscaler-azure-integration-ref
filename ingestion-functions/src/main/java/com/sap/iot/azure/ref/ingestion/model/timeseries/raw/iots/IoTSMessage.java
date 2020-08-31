package com.sap.iot.azure.ref.ingestion.model.timeseries.raw.iots;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class IoTSMessage {
    private String sensorAlternateId;
    private String capabilityAlternateId;
    private List<IoTSMessageMeasure> measures;
}
