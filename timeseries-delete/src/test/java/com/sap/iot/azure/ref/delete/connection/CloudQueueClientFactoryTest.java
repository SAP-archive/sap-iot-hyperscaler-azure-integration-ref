package com.sap.iot.azure.ref.delete.connection;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.util.Constants;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CloudQueueClientFactoryTest {

    @Mock
    private static CloudQueueClient cloudQueueClient;
    @Mock
    private static CloudQueue purgeQueue;

    @Spy
    CloudQueueClientFactory cloudQueueClientFactory = new CloudQueueClientFactory();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws URISyntaxException, InvalidKeyException {
        reset(cloudQueueClientFactory);
        doReturn(cloudQueueClient).when(cloudQueueClientFactory).createCloudQueueClient();
        cloudQueueClientFactory.initializeClass();
    }

    @Test
    public void testGetPurgeQueueClient() throws URISyntaxException, InvalidKeyException, StorageException {
        doReturn(purgeQueue).when(cloudQueueClient).getQueueReference(Constants.PURGE_QUEUE_NAME);
        CloudQueue result = cloudQueueClientFactory.getPurgeQueue();
        cloudQueueClientFactory.getPurgeQueue();

        verify(cloudQueueClientFactory, times(1)).createCloudQueueClient();
        verify(cloudQueueClient, times(1)).getQueueReference(Constants.PURGE_QUEUE_NAME);
        assertEquals(purgeQueue, result);
    }

    @Test
    public void testException() throws URISyntaxException, InvalidKeyException {
        doThrow(URISyntaxException.class).when(cloudQueueClientFactory).createCloudQueueClient();

        expectedException.expect(DeleteTimeSeriesException.class);
        cloudQueueClientFactory.getCloudQueueClient();
    }

    @Test
    public void testPurgeQueueException() throws URISyntaxException, StorageException, InvalidKeyException {
        doThrow(URISyntaxException.class).when(cloudQueueClient).getQueueReference(anyString());

        expectedException.expect(DeleteTimeSeriesException.class);
        cloudQueueClientFactory.getPurgeQueue();
    }
}