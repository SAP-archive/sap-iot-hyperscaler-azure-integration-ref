package com.sap.iot.azure.ref.delete.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
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
public class PurgeInfo {
    String structureId;
    List<DeleteInfo> deleteInfos;
    List<CloudQueueMessage> cloudQueueMessages;
}
