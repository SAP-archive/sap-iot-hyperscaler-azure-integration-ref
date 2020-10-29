package com.sap.iot.azure.ref.delete;

import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.model.cloudEvents.SapIoTAbstractionExtension;
import com.sap.iot.azure.ref.delete.storagequeue.OperationStorageQueueProcessor;
import com.sap.iot.azure.ref.integration.commons.adx.ADXTableManager;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import io.cloudevents.Attributes;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;

import java.util.Optional;

public class DeleteTimeSeriesHandler {

    private final ADXTableManager adxTableManager;
    private final OperationStorageQueueProcessor operationStorageQueueProcessor;

    public DeleteTimeSeriesHandler() {
        this(new ADXTableManager(), new OperationStorageQueueProcessor());
    }

    @VisibleForTesting
    DeleteTimeSeriesHandler(ADXTableManager adxTableManager, OperationStorageQueueProcessor operationStorageQueueProcessor) {
        this.adxTableManager = adxTableManager;
        this.operationStorageQueueProcessor = operationStorageQueueProcessor;
    }

    void processMessage(String message) {
        try {
            CloudEvent<Attributes, DeleteInfo> deleteRequest = Json.decodeValue(message, CloudEventImpl.class, DeleteInfo.class);
            DeleteInfo deleteInfo = deleteRequest.getData().orElseThrow(() -> new DeleteTimeSeriesException("Delete request info is missing in the message",
                    DeleteTimeSeriesErrorType.INVALID_MESSAGE, IdentifierUtil.getIdentifier("CloudEventId", deleteRequest.getAttributes().getId()), false));

            String operationId = deleteTimeSeries(deleteInfo);
            String structureId = deleteInfo.getStructureId();
            String correlationId = SapIoTAbstractionExtension.getExtension(deleteRequest).getCorrelationId();
            operationStorageQueueProcessor.process(operationStorageQueueProcessor.getOperationInfoMessage(operationId, deleteRequest.getAttributes().getId(),
                    structureId, correlationId), Optional.empty());
        } catch (IllegalStateException e) {
            throw new DeleteTimeSeriesException("Unable to parse message", e, DeleteTimeSeriesErrorType.INVALID_MESSAGE,
                    IdentifierUtil.empty(), false);
        }
    }

    private String deleteTimeSeries(DeleteInfo request) {
        return adxTableManager.deleteTimeSeries(request);
    }
}
