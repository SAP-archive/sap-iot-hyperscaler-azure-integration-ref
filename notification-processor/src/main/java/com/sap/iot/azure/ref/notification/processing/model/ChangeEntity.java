package com.sap.iot.azure.ref.notification.processing.model;

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
public class ChangeEntity {
    private EntityType type;
    private String entity;
    private String providerEntity;
    private ChangeEntityOperation operation;
    private List<String> descriptions;
    private List<DataEntity> additionalEntityData;
}
