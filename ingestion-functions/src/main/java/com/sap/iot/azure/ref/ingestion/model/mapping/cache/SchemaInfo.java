package com.sap.iot.azure.ref.ingestion.model.mapping.cache;

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

public class SchemaInfo implements Serializable {

    @NonNull
    private String structureId;

    private List<AvroTag> tags;
    private List<AvroMeasurement> measurements;

}
