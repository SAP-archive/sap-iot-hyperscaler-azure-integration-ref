package com.sap.iot.azure.ref.delete.connection;

import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CloudQueueClientFactoryTest {

    @Mock
    private static CloudQueueClient cloudQueueClient;

    @Spy
    CloudQueueClientFactory cloudQueueClientFactory = new CloudQueueClientFactory();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetCloudQueueClient() throws URISyntaxException, InvalidKeyException {
        doReturn(cloudQueueClient).when(cloudQueueClientFactory).createCloudQueueClient();
        cloudQueueClientFactory.getCloudQueueClient();
        cloudQueueClientFactory.getCloudQueueClient();

        verify(cloudQueueClientFactory, times(1)).createCloudQueueClient();
    }

    @Test
    public void testException() throws URISyntaxException, InvalidKeyException {
        doThrow(URISyntaxException.class).when(cloudQueueClientFactory).createCloudQueueClient();

        expectedException.expect(DeleteTimeSeriesException.class);
        cloudQueueClientFactory.getCloudQueueClient();
    }
}