package com.sap.iot.azure.ref.delete.logic;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.queue.QueueRequestOptions;
import com.sap.iot.azure.ref.delete.DeleteTimeSeriesTestUtil;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.model.OperationType;
import com.sap.iot.azure.ref.delete.storagequeue.OperationStorageQueueProcessor;
import com.sap.iot.azure.ref.delete.storagequeue.StorageQueueMessageInfo;
import com.sap.iot.azure.ref.delete.util.StatusQueueHelper;
import com.sap.iot.azure.ref.integration.commons.adx.ADXDataManager;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PurgeTimeSeriesHandlerTest {
    @Mock
    private ADXDataManager adxDataManager;
    @Mock
    private OperationStorageQueueProcessor operationStorageQueueProcessor;
    @Spy
    private RetryTaskExecutor retryTaskExecutor;
    @Mock
    private CloudQueueMessage cloudQueueMessage;
    @Mock
    private CloudQueue purgeQueue;
    @Mock
    private StatusQueueHelper statusQueueHelper;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private PurgeTimeSeriesHandler purgeTimeSeriesHandler;

    @Before
    public void setup() {
        String sampleOperationId = "SAMPLE_OPERATION_ID";
        purgeTimeSeriesHandler = new PurgeTimeSeriesHandler(adxDataManager, operationStorageQueueProcessor, purgeQueue, retryTaskExecutor, statusQueueHelper);
        doReturn(sampleOperationId).when(adxDataManager).purgeTimeSeries(anyString(), any(List.class));
        doReturn(cloudQueueMessage).when(operationStorageQueueProcessor).getOperationInfoMessage(sampleOperationId, DeleteTimeSeriesTestUtil.REQUEST_ID, DeleteTimeSeriesTestUtil.STRUCTURE_ID, DeleteTimeSeriesTestUtil.CORRELATION_ID,
                OperationType.PURGE);
    }

    @Test
    public void testPurgeSingleRequest() throws IOException, StorageException {
        CloudQueueMessage purgeCloudQueueMessage = getCloudQueueMessage(DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json"));

        purgeTimeSeriesHandler.processMessages(Collections.singletonList(purgeCloudQueueMessage));

        verify(adxDataManager, times(1)).purgeTimeSeries(anyString(), any(List.class));
        verify(operationStorageQueueProcessor, times(1)).apply(new StorageQueueMessageInfo(cloudQueueMessage, Optional.empty()));
    }

    @Test
    public void testConsolidation() throws IOException, StorageException {
        CloudQueueMessage purgeCloudQueueMessage = getCloudQueueMessage(DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json"));

        purgeTimeSeriesHandler.processMessages(Arrays.asList(purgeCloudQueueMessage, purgeCloudQueueMessage, purgeCloudQueueMessage));

        //Since all messages have the same structure ID, purge should only be called once
        verify(adxDataManager, times(1)).purgeTimeSeries(anyString(), any(List.class));
        //All 3 operations should be forwarded to the operation storage queue
        verify(operationStorageQueueProcessor, times(3)).apply(new StorageQueueMessageInfo(cloudQueueMessage, Optional.empty()));
    }

    @Test
    public void testInvalidMessage() throws IOException, StorageException {
        CloudQueueMessage invalidMessage = getCloudQueueMessage(DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/InvalidDeleteTimeSeriesRequest.json"));
        CloudQueueMessage validMessage = getCloudQueueMessage(DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json"));

        purgeTimeSeriesHandler.processMessages(Arrays.asList(invalidMessage, validMessage));
        verify(adxDataManager, times(1)).purgeTimeSeries(anyString(), any(List.class));
        verify(operationStorageQueueProcessor, times(1)).apply(new StorageQueueMessageInfo(cloudQueueMessage, Optional.empty()));
    }

    @Test
    public void testDequeueException() throws IOException, StorageException {
        CloudQueueMessage message = getCloudQueueMessage(DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json"));

        doThrow(StorageException.class).when(purgeQueue).deleteMessage(any(CloudQueueMessage.class), any(QueueRequestOptions.class), any());

        //Error is handled by logging it
        purgeTimeSeriesHandler.processMessages(Collections.singletonList(message));
    }

    @Test
    public void testADXException() throws IOException, StorageException {
        CloudQueueMessage purgeCloudQueueMessage = getCloudQueueMessage(DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json"));
        ADXClientException testException = new ADXClientException("Test Exception", IdentifierUtil.empty(), false);

        doThrow(testException).when(retryTaskExecutor).executeWithRetry(any(), anyInt());
        try {
            purgeTimeSeriesHandler.processMessages(Arrays.asList(purgeCloudQueueMessage, purgeCloudQueueMessage, purgeCloudQueueMessage));
        }catch (DeleteTimeSeriesException e) {
            verify(statusQueueHelper, times(3)).sendFailedDeleteToStatusQueue(any(DeleteInfo.class), eq(false));
        }
    }

    private CloudQueueMessage getCloudQueueMessage(String message) throws StorageException, IOException {
        CloudQueueMessage purgeCloudQueueMessage = mock(CloudQueueMessage.class);
        doReturn(message).when(purgeCloudQueueMessage).getMessageContentAsString();

        return purgeCloudQueueMessage;
    }
}