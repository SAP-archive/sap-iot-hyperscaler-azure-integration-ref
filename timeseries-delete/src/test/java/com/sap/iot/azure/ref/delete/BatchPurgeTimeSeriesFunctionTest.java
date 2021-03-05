package com.sap.iot.azure.ref.delete;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.queue.QueueRequestOptions;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.logic.PurgeTimeSeriesHandler;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BatchPurgeTimeSeriesFunctionTest {
    @Mock
    private PurgeTimeSeriesHandler purgeTimeSeriesHandler;
    @Mock
    private CloudQueue purgeQueue;
    @Mock
    private CloudQueueMessage cloudQueueMessage;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private String sampleTimerInfo = "";

    @Test
    public void testRun() throws StorageException {
        List<CloudQueueMessage> cloudQueueMessages = Collections.singletonList(cloudQueueMessage);
        List emptyMessages = Collections.emptyList();

        doReturn(cloudQueueMessages, emptyMessages).when(purgeQueue).retrieveMessages(anyInt(), anyInt(), any(QueueRequestOptions.class), any());

        BatchPurgeTimeSeriesFunction batchPurgeTimeSeriesFunction = new BatchPurgeTimeSeriesFunction(purgeTimeSeriesHandler, purgeQueue);

        batchPurgeTimeSeriesFunction.run(sampleTimerInfo, InvocationContextTestUtil.getMockContext());
        verify(purgeTimeSeriesHandler, times(1)).processMessages(cloudQueueMessages);
    }

    @Test
    public void testRunLoop() throws StorageException {
        List<CloudQueueMessage> cloudQueueMessages = Collections.singletonList(cloudQueueMessage);
        List emptyMessages = Collections.emptyList();

        doReturn(cloudQueueMessages, cloudQueueMessages, emptyMessages).when(purgeQueue).retrieveMessages(anyInt(), anyInt(), any(QueueRequestOptions.class), any());

        BatchPurgeTimeSeriesFunction batchPurgeTimeSeriesFunction = new BatchPurgeTimeSeriesFunction(purgeTimeSeriesHandler, purgeQueue);

        batchPurgeTimeSeriesFunction.run(sampleTimerInfo, InvocationContextTestUtil.getMockContext());
        verify(purgeTimeSeriesHandler, times(2)).processMessages(cloudQueueMessages);
    }

    @Test
    public void testException() throws StorageException {
        doThrow(StorageException.class).when(purgeQueue).retrieveMessages(anyInt(), anyInt(), any(QueueRequestOptions.class), any());

        BatchPurgeTimeSeriesFunction batchPurgeTimeSeriesFunction = new BatchPurgeTimeSeriesFunction(purgeTimeSeriesHandler, purgeQueue);

        expectedException.expect(DeleteTimeSeriesException.class);
        batchPurgeTimeSeriesFunction.run(sampleTimerInfo, InvocationContextTestUtil.getMockContext());
    }
}