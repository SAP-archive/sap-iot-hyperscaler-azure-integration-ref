package com.sap.iot.azure.ref.delete.storagequeue;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.queue.QueueRequestOptions;
import com.sap.iot.azure.ref.delete.util.Constants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PurgeStorageQueueProcessorTest {
    @Mock
    CloudQueueClient cloudQueueClient;
    @InjectMocks
    PurgeStorageQueueProcessor purgeStorageQueueProcessor;
    @Mock
    CloudQueue cloudQueue;
    @Mock
    CloudQueueMessage message;
    @Captor
    private ArgumentCaptor<CloudQueueMessage> messageCaptor;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Test
    public void testRun() throws StorageException, URISyntaxException {
        doReturn(cloudQueue).when(cloudQueueClient).getQueueReference(anyString());

        purgeStorageQueueProcessor.apply(message);

        verify(cloudQueue, times(1)).addMessage(messageCaptor.capture(), eq(0), eq(0), any(QueueRequestOptions.class), isNull());
        assertEquals(message, messageCaptor.getValue());
    }

    @Test
    public void testStorageException() throws StorageException, URISyntaxException {
        doReturn(cloudQueue).when(cloudQueueClient).getQueueReference(anyString());

        doThrow(StorageException.class).when(cloudQueue).addMessage(any(CloudQueueMessage.class), anyInt(), anyInt(), any(QueueRequestOptions.class), isNull());
        //Since the exception is non transient, the processor should handle it. This Exception is not expected to be rethrown.
        purgeStorageQueueProcessor.apply(message);
    }

    @Test
    public void testURISyntaxException() throws StorageException, URISyntaxException {
        doThrow(URISyntaxException.class).when(cloudQueueClient).getQueueReference(anyString());
        //Since the exception is non transient, the processor should handle it. This Exception is not expected to be rethrown.
        purgeStorageQueueProcessor.apply(message);
    }

}