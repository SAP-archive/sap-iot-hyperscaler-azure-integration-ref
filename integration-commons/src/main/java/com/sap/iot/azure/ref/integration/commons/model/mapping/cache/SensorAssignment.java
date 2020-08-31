package com.sap.iot.azure.ref.integration.commons.model.mapping.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SensorAssignment {
    @NonNull
    private String sensorId;
    @NonNull
    private String assignmentId;
    @NonNull
    private String mappingId;
    @NonNull
    private String objectId;
}