package com.sap.iot.azure.ref.integration.commons.model.mapping.api.mapping;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMapping;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MappingEndpointResponse {
    //structureId, capabilityId and mappingInfo from mapping endpoint
    @JsonProperty("StructureId")
    private String structureId;
    @JsonProperty("CapabilityId")
    private String capabilityId;
    @JsonProperty("PropertyMeasures")
    @JsonDeserialize(using = PropertyMappingsJsonDeserializer.class)
    private List<PropertyMapping> propertyMappings;
}
