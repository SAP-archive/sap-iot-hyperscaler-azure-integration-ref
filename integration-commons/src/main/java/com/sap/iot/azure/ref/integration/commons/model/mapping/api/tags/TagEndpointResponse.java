package com.sap.iot.azure.ref.integration.commons.model.mapping.api.tags;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TagEndpointResponse {
    private String sourceId;
    private String structureId;
    @JsonProperty("tag")
    private List<Tag> tags;
}
