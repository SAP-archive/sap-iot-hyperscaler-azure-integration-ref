package com.sap.iot.azure.ref.device.management.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.sap.iot.azure.ref.device.management.model.cloudevents.SAPIoTCloudEventType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceManagementStatusInfo {

    private SAPIoTCloudEventType sourceEventType;
    private String sourceEventTransactionId;
    private String sourceEventSequenceNumber;
    private DeviceManagementStatus deviceManagementStatus;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DeviceManagementStatus {
        private String deviceId;
        private String status;
        private Error error;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Error {
        private String code;
        private String message;
    }
}
