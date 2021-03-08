package com.sap.iot.azure.ref.delete.storagequeue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.model.DeleteMonitoringCloudQueueMessage;
import com.sap.iot.azure.ref.delete.model.DeleteStatusMessage;
import com.sap.iot.azure.ref.delete.model.DeleteStatus;
import com.sap.iot.azure.ref.delete.model.OperationInfo;
import com.sap.iot.azure.ref.delete.model.OperationType;
import com.sap.iot.azure.ref.delete.output.DeleteStatusEventHubProcessor;
import com.sap.iot.azure.ref.integration.commons.adx.ADXDataManager;
import com.sap.iot.azure.ref.integration.commons.adx.ADXTableManager;
import com.sap.iot.azure.ref.integration.commons.adx.DeleteOperationStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Date;
import java.util.Optional;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)

public class DeleteMonitoringProcessorTest {

    @Mock
    private ADXDataManager adxDataManager;
    @Mock
    private DeleteStatusEventHubProcessor deleteStatusEventHubProcessor;
    @Mock
    private OperationStorageQueueProcessor operationStorageQueueProcessor;
    @InjectMocks
    DeleteMonitoringProcessor deleteMonitoringProcessor;
    @Mock
    DeleteMonitoringCloudQueueMessage deleteMonitoringCloudQueueMessage;

    private static final ObjectMapper mapper = new ObjectMapper();

    String OPERATION_ID = "SAMPLE_OPERATION_ID";
    private final String CORRELATION_ID = "SAMPLE_CORRELATION_ID";
    private final String EVENT_ID = "SAMPLE_EVENT_ID";
    private final String STRUCTURE_ID = "SAMPLE_STRUCTURE_ID";
    private static final OperationType OPERATION_TYPE_SOFT = OperationType.SOFT_DELETE;
    private static final OperationType OPERATION_TYPE_PURGE = OperationType.PURGE;

    private final Date NEXT_VISIBLE_TIME = new Date(System.currentTimeMillis());

    private CloudQueueMessage message;
    OperationInfo operationInfo = OperationInfo.builder().operationId(OPERATION_ID).correlationId(CORRELATION_ID).eventId(EVENT_ID).structureId
            (STRUCTURE_ID).operationType(OPERATION_TYPE_SOFT).build();
    OperationInfo operationInfoPurge = OperationInfo.builder().operationId(OPERATION_ID).correlationId(CORRELATION_ID).eventId(EVENT_ID).structureId
            (STRUCTURE_ID).operationType(OPERATION_TYPE_PURGE).build();
    @Before
    public void setup() throws JsonProcessingException {
        deleteMonitoringCloudQueueMessage = DeleteMonitoringCloudQueueMessage.builder().operationInfo(mapper.writeValueAsString(operationInfo))
                .nextVisibleTime(NEXT_VISIBLE_TIME.getTime()).build();
    }

    @Test
    public void testProcessDeleteComplete() {
        DeleteStatusMessage deleteStatusMessage = new DeleteStatusMessage();
        deleteStatusMessage.setCorrelationId(CORRELATION_ID);
        deleteStatusMessage.setStructureId(STRUCTURE_ID);
        deleteStatusMessage.setEventId(EVENT_ID);
        deleteStatusMessage.setStatus(DeleteStatus.SUCCESS);
        Mockito.when(adxDataManager.getDeleteOperationStatus(OPERATION_ID, STRUCTURE_ID)).thenReturn(DeleteOperationStatus.COMPLETED);
        deleteMonitoringProcessor.process(deleteMonitoringCloudQueueMessage);
        verify(adxDataManager, times(1)).getDeleteOperationStatus(OPERATION_ID, STRUCTURE_ID);
        verify(deleteStatusEventHubProcessor, times(1)).apply(deleteStatusMessage);
    }

    @Test
    public void testProcessDeleteFailed() {
        DeleteStatusMessage deleteStatusMessage = new DeleteStatusMessage();
        deleteStatusMessage.setCorrelationId(CORRELATION_ID);
        deleteStatusMessage.setStructureId(STRUCTURE_ID);
        deleteStatusMessage.setEventId(EVENT_ID);
        deleteStatusMessage.setStatus(DeleteStatus.FAILED);
        deleteStatusMessage.setError("Delete operation failed for operation id: " + OPERATION_ID);
        Mockito.when(adxDataManager.getDeleteOperationStatus(OPERATION_ID, STRUCTURE_ID)).thenReturn(DeleteOperationStatus.FAILED);
        deleteMonitoringProcessor.process(deleteMonitoringCloudQueueMessage);
        verify(deleteStatusEventHubProcessor, times(1)).apply(deleteStatusMessage);
    }

    @Test
    public void testProcessDeleteInProgress() {
        Mockito.when(adxDataManager.getDeleteOperationStatus(OPERATION_ID, STRUCTURE_ID)).thenReturn(DeleteOperationStatus.INPROGRESS);
        Mockito.when(operationStorageQueueProcessor.getOperationInfoMessage(OPERATION_ID, EVENT_ID, STRUCTURE_ID, CORRELATION_ID, OPERATION_TYPE_SOFT)).thenReturn(message);
        deleteMonitoringProcessor.process(deleteMonitoringCloudQueueMessage);
        verify(operationStorageQueueProcessor, times(1)).getOperationInfoMessage(OPERATION_ID, EVENT_ID, STRUCTURE_ID, CORRELATION_ID, OPERATION_TYPE_SOFT);
        message = operationStorageQueueProcessor.getOperationInfoMessage(OPERATION_ID, EVENT_ID, STRUCTURE_ID, CORRELATION_ID, OPERATION_TYPE_SOFT);
        verify(operationStorageQueueProcessor, times(1)).apply(new StorageQueueMessageInfo(message, Optional.of(NEXT_VISIBLE_TIME)));
    }

    @Test
    public void testPurgeQueryMonitoring() throws JsonProcessingException{
        deleteMonitoringCloudQueueMessage = DeleteMonitoringCloudQueueMessage.builder().operationInfo(mapper.writeValueAsString(operationInfoPurge))
                .nextVisibleTime(NEXT_VISIBLE_TIME.getTime()).build();
        DeleteStatusMessage deleteStatusMessage = new DeleteStatusMessage();
        deleteStatusMessage.setCorrelationId(CORRELATION_ID);
        deleteStatusMessage.setStructureId(STRUCTURE_ID);
        deleteStatusMessage.setEventId(EVENT_ID);
        deleteStatusMessage.setStatus(DeleteStatus.SUCCESS);
        Mockito.when(adxDataManager.getPurgeOperationStatus(OPERATION_ID, STRUCTURE_ID)).thenReturn(DeleteOperationStatus.COMPLETED);
        deleteMonitoringProcessor.process(deleteMonitoringCloudQueueMessage);
        verify(adxDataManager, times(1)).getPurgeOperationStatus(OPERATION_ID, STRUCTURE_ID);
        verify(deleteStatusEventHubProcessor, times(1)).apply(deleteStatusMessage);
    }
}