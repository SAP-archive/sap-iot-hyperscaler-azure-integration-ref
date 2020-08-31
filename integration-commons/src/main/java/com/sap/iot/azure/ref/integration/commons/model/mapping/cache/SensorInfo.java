package com.sap.iot.azure.ref.integration.commons.model.mapping.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor

public class SensorInfo {
    @NonNull
    private String sensorId;
    @NonNull
    private String virtualCapabilityId;
    private String sourceId;
    private String structureId;
    private List<Tag> tags;
    private String mappingId;
}
