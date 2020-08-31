package com.sap.iot.azure.ref.integration.commons.model.mapping.cache;
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
public class Tag implements Serializable {

    @JsonProperty("TagSemantic")
    private String tagSemantic;

    @JsonProperty("TagValue")
    private String tagValue;
}
