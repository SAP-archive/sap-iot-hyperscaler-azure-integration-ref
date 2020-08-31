package com.sap.iot.azure.ref.integration.commons.model.mapping.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PropertyMappingInfo {
    @NonNull
    private String mappingId;
    @NonNull
    private String structureId;
    @NonNull
    private String virtualCapabilityId;
    private List<PropertyMapping> propertyMappings;
}
