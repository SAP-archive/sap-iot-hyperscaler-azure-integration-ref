package com.sap.iot.azure.ref.integration.commons.model.mapping.api.assignment;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
@NoArgsConstructor
public class AssignmentEndpointResponse {
    @JsonProperty("MappingId")
    private String mappingId;
    @JsonProperty("AssignmentId")
    private String assignmentId;
}
