package com.sap.iot.azure.ref.ingestion.model.mapping.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor

public class AvroMeasurement implements Serializable {

    private String name;
    private String type;
    private String logicalType;
}
