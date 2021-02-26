package com.sap.iot.azure.ref.notification.processing;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.sap.iot.azure.ref.integration.commons.model.MessageEntity;
import com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties;
import com.sap.iot.azure.ref.notification.processing.model.ChangeEntity;
import com.sap.iot.azure.ref.notification.processing.model.DataEntity;
import com.sap.iot.azure.ref.notification.processing.model.EntityType;
import com.sap.iot.azure.ref.notification.processing.model.OperationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationMessage implements MessageEntity<SystemProperties> {
    private EntityType type;
    private OperationType operation;
    private String changeEntity;
    private List<ChangeEntity> changeList;
    private List<DataEntity> entityDataList;
    private String partitionKey;

    // message source info
    private SystemProperties source;
}





