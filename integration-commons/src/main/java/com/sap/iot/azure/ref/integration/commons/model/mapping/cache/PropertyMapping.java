package com.sap.iot.azure.ref.integration.commons.model.mapping.cache;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PropertyMapping implements Serializable {
    @JsonProperty("StructurePropertyId")
    private String structurePropertyId;
    @JsonProperty("CapabilityPropertyId")
    private String capabilityPropertyId;
}
