package com.sap.iot.azure.ref.delete.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OperationInfo {
    private String operationId;
    private String eventId;
    private String structureId;
    private String correlationId;
    private OperationType operationType;
}
