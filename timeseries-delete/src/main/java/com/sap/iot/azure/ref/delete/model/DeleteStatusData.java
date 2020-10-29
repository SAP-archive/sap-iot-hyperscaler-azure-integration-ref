package com.sap.iot.azure.ref.delete.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeleteStatusData {
    private DeleteStatustoEventhub status;
    private String eventId;
    private String error;
}
