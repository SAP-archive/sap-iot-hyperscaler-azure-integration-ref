package com.sap.iot.azure.ref.integration.commons.model.mapping;

import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMapping;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.Tag;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder

public class SensorMappingInfo {
    private String sourceId;
    private String structureId;
    private List<Tag> tags;
    private List<PropertyMapping> propertyMappings;
    private String schemaInfo;
}
