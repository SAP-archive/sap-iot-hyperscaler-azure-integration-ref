package com.sap.iot.azure.ref.delete.storagequeue;

import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.model.DeleteMonitoringCloudQueueMessage;
import com.sap.iot.azure.ref.delete.model.DeleteStatusMessage;
import com.sap.iot.azure.ref.delete.model.DeleteStatustoEventhub;
import com.sap.iot.azure.ref.delete.output.DeleteStatusEventHubProcessor;
import com.sap.iot.azure.ref.integration.commons.adx.ADXTableManager;
import com.sap.iot.azure.ref.integration.commons.adx.DeleteOperationStatus;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;

import java.util.Optional;

public class DeleteMonitoringProcessor implements Processor<DeleteMonitoringCloudQueueMessage, Void> {

    private final ADXTableManager adxTableManager;
    private final DeleteStatusEventHubProcessor deleteStatusEventHubProcessor;
    private  final OperationStorageQueueProcessor operationStorageQueueProcessor;
    public DeleteMonitoringProcessor() {
        this(new ADXTableManager(), new DeleteStatusEventHubProcessor(), new OperationStorageQueueProcessor());
    }
    DeleteMonitoringProcessor(ADXTableManager adxTableManager, DeleteStatusEventHubProcessor
            deleteStatusEventHubProcessor, OperationStorageQueueProcessor operationStorageQueueProcessor) {
        this.adxTableManager = adxTableManager;
        this.deleteStatusEventHubProcessor = deleteStatusEventHubProcessor;
        this.operationStorageQueueProcessor = operationStorageQueueProcessor;
    }

    @Override
    public Void process(DeleteMonitoringCloudQueueMessage deleteMonitoringCloudQueueMessage) {
        DeleteStatusMessage deleteStatusMessage = new DeleteStatusMessage();
        try {
            // Get status for operation id
            String operationId = deleteMonitoringCloudQueueMessage.getOperationInfo().getOperationId();
            String eventId = deleteMonitoringCloudQueueMessage.getOperationInfo().getEventId();
            String structureId = deleteMonitoringCloudQueueMessage.getOperationInfo().getStructureId();
            String correlationId = deleteMonitoringCloudQueueMessage.getOperationInfo().getCorrelationId();
            DeleteOperationStatus deleteOperationStatus = adxTableManager.getDeleteOperationStatus(operationId, structureId);
            if (deleteOperationStatus.equals(DeleteOperationStatus.COMPLETED)) {
                deleteStatusMessage.setStatus(DeleteStatustoEventhub.SUCCESS);
                deleteStatusMessage.setEventId(eventId);
                deleteStatusMessage.setStructureId(structureId);
                deleteStatusMessage.setCorrelationId(correlationId);
                //write to EventHub
                deleteStatusEventHubProcessor.apply(deleteStatusMessage);
            } else if(deleteOperationStatus.equals(DeleteOperationStatus.FAILED) || deleteOperationStatus.equals(DeleteOperationStatus.BADINPUT)){
                deleteStatusMessage.setStatus(DeleteStatustoEventhub.FAILED);
                deleteStatusMessage.setEventId(eventId);
                deleteStatusMessage.setStructureId(structureId);
                deleteStatusMessage.setError("Delete operation failed for operation id: " + operationId);
                deleteStatusMessage.setCorrelationId(correlationId);
                //write to EventHub
                deleteStatusEventHubProcessor.apply(deleteStatusMessage);
            } else {
                //If delete in process -> add timespan and write back into queue to continue monitoring
                CloudQueueMessage message = operationStorageQueueProcessor.getOperationInfoMessage(operationId, eventId, structureId, correlationId);
                operationStorageQueueProcessor.process(message, Optional.ofNullable(deleteMonitoringCloudQueueMessage.getNextVisibleTime()));
            }
        } catch (ADXClientException e) {
            e.addIdentifier("operationId", deleteMonitoringCloudQueueMessage.getOperationInfo().getOperationId());
            e.addIdentifier("eventId", deleteMonitoringCloudQueueMessage.getOperationInfo().getEventId());
            throw e;
        }
        return null; //required to implement the processor interface
    }
}
