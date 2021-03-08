package com.sap.iot.azure.ref.delete.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeleteStatusMessage {
    private DeleteStatus status;
    private String eventId;
    private String error;
    private String structureId;
    private String correlationId;

}
