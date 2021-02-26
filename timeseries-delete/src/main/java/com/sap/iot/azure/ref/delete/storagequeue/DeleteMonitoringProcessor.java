package com.sap.iot.azure.ref.delete.storagequeue;

import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.model.DeleteMonitoringCloudQueueMessage;
import com.sap.iot.azure.ref.delete.model.DeleteStatus;
import com.sap.iot.azure.ref.delete.model.DeleteStatusMessage;
import com.sap.iot.azure.ref.delete.model.OperationType;
import com.sap.iot.azure.ref.delete.output.DeleteStatusEventHubProcessor;
import com.sap.iot.azure.ref.integration.commons.adx.ADXDataManager;
import com.sap.iot.azure.ref.integration.commons.adx.DeleteOperationStatus;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;

import java.util.Optional;

public class DeleteMonitoringProcessor implements Processor<DeleteMonitoringCloudQueueMessage, Void> {

    private final ADXDataManager adxDataManager;
    private final DeleteStatusEventHubProcessor deleteStatusEventHubProcessor;
    private final OperationStorageQueueProcessor operationStorageQueueProcessor;

    public DeleteMonitoringProcessor() {
        this(new ADXDataManager(), new DeleteStatusEventHubProcessor(), new OperationStorageQueueProcessor());
    }
  
    DeleteMonitoringProcessor(ADXDataManager adxDataManager, DeleteStatusEventHubProcessor
            deleteStatusEventHubProcessor, OperationStorageQueueProcessor operationStorageQueueProcessor) {
        this.adxDataManager = adxDataManager;
        this.deleteStatusEventHubProcessor = deleteStatusEventHubProcessor;
        this.operationStorageQueueProcessor = operationStorageQueueProcessor;
    }

    /**
     * Processes a delete monitoring message.
     * Depending on the operation type of the message, an ADX query is formed and executed.
     * If the delete operation is still pending, the monitoring message is written back into the queue.
     * If the operation is completed, the status is sent to the status Event Hub.
     *
     * @param deleteMonitoringCloudQueueMessage monitoring message
     */
    @Override
    public Void process(DeleteMonitoringCloudQueueMessage deleteMonitoringCloudQueueMessage) {
        DeleteStatusMessage deleteStatusMessage = new DeleteStatusMessage();
        try {
            // Get status for operation id
            String operationId = deleteMonitoringCloudQueueMessage.getOperationInfo().getOperationId();
            String eventId = deleteMonitoringCloudQueueMessage.getOperationInfo().getEventId();
            String structureId = deleteMonitoringCloudQueueMessage.getOperationInfo().getStructureId();
            String correlationId = deleteMonitoringCloudQueueMessage.getOperationInfo().getCorrelationId();
            OperationType operationType = deleteMonitoringCloudQueueMessage.getOperationInfo().getOperationType();
            DeleteOperationStatus deleteOperationStatus;
            if (operationType.equals(OperationType.PURGE)) {
                deleteOperationStatus = adxDataManager.getPurgeOperationStatus(operationId, structureId);
            } else {
                deleteOperationStatus = adxDataManager.getDeleteOperationStatus(operationId, structureId);
            }
            if (deleteOperationStatus.equals(DeleteOperationStatus.COMPLETED)) {
                deleteStatusMessage.setStatus(DeleteStatus.SUCCESS);
                deleteStatusMessage.setEventId(eventId);
                deleteStatusMessage.setStructureId(structureId);
                deleteStatusMessage.setCorrelationId(correlationId);
                //write to EventHub
                deleteStatusEventHubProcessor.apply(deleteStatusMessage);
            } else if (deleteOperationStatus.equals(DeleteOperationStatus.FAILED) || deleteOperationStatus.equals(DeleteOperationStatus.BADINPUT)) {
                deleteStatusMessage.setStatus(DeleteStatus.FAILED);
                deleteStatusMessage.setEventId(eventId);
                deleteStatusMessage.setStructureId(structureId);
                deleteStatusMessage.setError("Delete operation failed for operation id: " + operationId);
                deleteStatusMessage.setCorrelationId(correlationId);
                //write to EventHub
                deleteStatusEventHubProcessor.apply(deleteStatusMessage);
            } else {
                //If delete in process -> add timespan and write back into queue to continue monitoring
                CloudQueueMessage message = operationStorageQueueProcessor.getOperationInfoMessage(operationId, eventId, structureId, correlationId, operationType);
                operationStorageQueueProcessor.apply(new StorageQueueMessageInfo(message, Optional.ofNullable(deleteMonitoringCloudQueueMessage.getNextVisibleTime())));
            }
        } catch (ADXClientException e) {
            e.addIdentifier("operationId", deleteMonitoringCloudQueueMessage.getOperationInfo().getOperationId());
            e.addIdentifier("eventId", deleteMonitoringCloudQueueMessage.getOperationInfo().getEventId());
            throw e;
        }
        return null; //required to implement the processor interface
    }
}
