package com.sap.iot.azure.ref.delete.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
public class DeleteMonitoringCloudQueueMessage {
    private final OperationInfo operationInfo;
    private final Date nextVisibleTime;

    private DeleteMonitoringCloudQueueMessage(OperationInfo operationInfo, Date nextVisibleTime) {
        this.operationInfo = operationInfo;
        this.nextVisibleTime = nextVisibleTime;
    }

    public Date getNextVisibleTime() {
        if (nextVisibleTime == null) {
            return null;
        } else {
            return new Date(nextVisibleTime.getTime());
        }
    }

    public static class DeleteMonitoringCloudQueueMessageBuilder{
        private static final ObjectMapper mapper = new ObjectMapper();
        public DeleteMonitoringCloudQueueMessageBuilder operationInfo(String message){
            try {
                this.operationInfo = mapper.readValue(message, OperationInfo.class);
            } catch (JsonProcessingException e) {
                throw new DeleteTimeSeriesException("Invalid delete operation message", DeleteTimeSeriesErrorType.JSON_PROCESSING_ERROR,
                        IdentifierUtil.empty(), false);
            }
            return this;
        }

        public DeleteMonitoringCloudQueueMessageBuilder nextVisibleTime(long epochMillis) {
            this.nextVisibleTime = new Date(epochMillis);
            return this;
        }
    }
}
