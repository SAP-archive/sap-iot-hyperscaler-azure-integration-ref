package com.sap.iot.azure.ref.delete.storagequeue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.eventhubs.impl.Operation;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.queue.QueueRequestOptions;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.model.OperationType;
import com.sap.iot.azure.ref.delete.util.Constants;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URISyntaxException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class OperationStorageQueueProcessorTest {
    @Mock
    CloudQueueClient cloudQueueClient;
    @Mock
    CloudQueue cloudQueue;
    @Spy
    ObjectMapper mapper;
    @InjectMocks
    OperationStorageQueueProcessor operationStorageQueueProcessor;
    @Captor
    private ArgumentCaptor<CloudQueueMessage> messageCaptor;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final ObjectMapper testMapper = new ObjectMapper();
    private static final String OPERATION_ID = "operation";
    private static final String EVENT_ID = "event";
    private static final String STRUCTURE_ID = "structure";
    private static final String CORRELATION_ID = "correlation";
    private static final OperationType OPERATION_TYPE = OperationType.SOFT_DELETE;
    private CloudQueueMessage MESSAGE;

    @Before
    public void setup() {
        MESSAGE = operationStorageQueueProcessor.getOperationInfoMessage(OPERATION_ID, EVENT_ID, STRUCTURE_ID, CORRELATION_ID, OPERATION_TYPE);
    }


    @Test
    public void testRun() throws StorageException, URISyntaxException {
        doReturn(cloudQueue).when(cloudQueueClient).getQueueReference(anyString());

        operationStorageQueueProcessor.apply(new StorageQueueMessageInfo(MESSAGE, Optional.empty()));

        verify(cloudQueue, times(1)).addMessage(messageCaptor.capture(), eq(0), eq(Constants.INITIAL_OPERATION_QUEUE_DELAY), any(QueueRequestOptions.class),
                isNull());
        assertEquals(MESSAGE, messageCaptor.getValue());
    }

    @Test
    public void testStorageException() throws StorageException, URISyntaxException {
        doReturn(cloudQueue).when(cloudQueueClient).getQueueReference(anyString());

        doThrow(StorageException.class).when(cloudQueue).addMessage(any(CloudQueueMessage.class), anyInt(), anyInt(), any(QueueRequestOptions.class), isNull());
        //Since the exception is non transient, the processor should handle it. This Exception is not expected to be rethrown.
        operationStorageQueueProcessor.apply(new StorageQueueMessageInfo(MESSAGE, Optional.empty()));
    }

    @Test
    public void testURISyntaxException() throws StorageException, URISyntaxException {
        doThrow(URISyntaxException.class).when(cloudQueueClient).getQueueReference(anyString());
        //Since the exception is non transient, the processor should handle it. This Exception is not expected to be rethrown.
        operationStorageQueueProcessor.apply(new StorageQueueMessageInfo(MESSAGE, Optional.empty()));
    }

    @Test
    public void testJsonProcessingException() throws JsonProcessingException, URISyntaxException, StorageException {
        doThrow(JsonProcessingException.class).when(mapper).writeValueAsString(any());
        expectedException.expect(DeleteTimeSeriesException.class);
        operationStorageQueueProcessor.getOperationInfoMessage(OPERATION_ID, EVENT_ID, STRUCTURE_ID, CORRELATION_ID, OPERATION_TYPE);
    }
}