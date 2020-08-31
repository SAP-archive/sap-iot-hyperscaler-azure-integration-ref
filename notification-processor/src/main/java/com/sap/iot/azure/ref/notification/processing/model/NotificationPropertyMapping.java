package com.sap.iot.azure.ref.notification.processing.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
public class NotificationPropertyMapping {
    private String structurePropertyId;
    private String capabilityPropertyId;
    private PropertyMappingOperation operation;
}
