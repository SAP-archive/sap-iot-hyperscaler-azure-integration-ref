package com.sap.iot.azure.ref.integration.commons.connection;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class EventHubClientFactoryTest {

    @Mock
    EventHubClient ehclientMock;

    @Mock
    ScheduledExecutorService executorService;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupClass() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Test
    public void testShutdownHooks() throws EventHubException {
        EventHubClientFactory eventHubClientFactory = new EventHubClientFactory();
        eventHubClientFactory.shutdownClient(ehclientMock);
        eventHubClientFactory.shutdownExecutorService(executorService);

        verify(ehclientMock, times(1)).closeSync();
        verify(executorService, times(1)).shutdown();
    }

    @Test
    public void testShutdownException() throws EventHubException {
        EventHubClientFactory eventHubClientFactory = new EventHubClientFactory();
        // We only log for now
        doThrow(EventHubException.class).when(ehclientMock).closeSync();

        eventHubClientFactory.shutdownClient(ehclientMock);
        eventHubClientFactory.shutdownExecutorService(executorService);
    }

    @Test
    public void testCreate() throws IOException, EventHubException {
        String sampleConnString = "123";
        EventHubClientFactory eventHubClientFactory = spy(new EventHubClientFactory());
        doReturn(CompletableFuture.completedFuture(ehclientMock)).when(eventHubClientFactory).createEventHubClient(eq(sampleConnString),
                any(ScheduledExecutorService.class));

        eventHubClientFactory.getEhClient(sampleConnString);
        eventHubClientFactory.getEhClient(sampleConnString);

        verify(eventHubClientFactory, times(1)).createEventHubClient(eq(sampleConnString), any());
    }

    @Test
    public void testCreateException() throws IOException, EventHubException {
        String sampleConnString = "123";
        EventHubClientFactory eventHubClientFactory = spy(new EventHubClientFactory());
        doThrow(EventHubException.class).when(eventHubClientFactory).createEventHubClient(eq(sampleConnString), any(ScheduledExecutorService.class));

        //Exception is rethrown as IngestionRuntimeException
        expectedException.expect(IoTRuntimeException.class);
        eventHubClientFactory.getEhClient(sampleConnString);
    }
}