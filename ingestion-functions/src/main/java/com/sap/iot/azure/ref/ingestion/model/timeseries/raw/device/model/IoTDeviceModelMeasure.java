package com.sap.iot.azure.ref.ingestion.model.timeseries.raw.device.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;
import java.util.Map;

public class IoTDeviceModelMeasure {
    private Map<String, Object> properties= new HashMap<String, Object>();

    public Map<String, Object> getProperties() {
        return properties;
    }

    @JsonAnySetter
    @SuppressWarnings("unused") // Used by Jackson
    public void setProperties(String name, Object value) {
        properties.put(name, value);
    }
}
