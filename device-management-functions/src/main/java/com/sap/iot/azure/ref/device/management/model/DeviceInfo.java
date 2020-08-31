package com.sap.iot.azure.ref.device.management.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceInfo {

    private String deviceId;
    private List<SensorEntity> sensors;
    private List<CapabilityEntity> capabilities;
}
