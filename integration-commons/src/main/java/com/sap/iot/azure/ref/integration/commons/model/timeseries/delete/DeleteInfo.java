package com.sap.iot.azure.ref.integration.commons.model.timeseries.delete;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeleteInfo {
    private List<String> sourceIds;
    private String structureId;
    private String ingestionTimestamp;
    private String fromTimestamp;
    private String toTimestamp;
    private Boolean fromTimestampInclusive;
    private Boolean toTimestampInclusive;
}
