package com.sap.iot.azure.ref.integration.commons.mapping.api;

import com.microsoft.azure.eventhubs.EventHubException;
import org.asynchttpclient.AsyncHttpClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AsyncHttpClientFactoryTest {

    @Mock
    AsyncHttpClient asyncHttpClient;

    @Test
    public void testShutdownHooks() throws EventHubException, IOException {
        AsyncHttpClientFactory asyncHttpClientFactory = Mockito.spy(new AsyncHttpClientFactory());
        asyncHttpClientFactory.shutdownClient(asyncHttpClient);

        verify(asyncHttpClient, times(1)).close();
    }

    @Test
    public void testShutdownException() throws EventHubException, IOException {
        AsyncHttpClientFactory asyncHttpClientFactory = new AsyncHttpClientFactory();
        // We only log for now
        doThrow(IOException.class).when(asyncHttpClient).close();

        asyncHttpClientFactory.shutdownClient(asyncHttpClient);
    }

    @Test
    public void testCreate() {
        AsyncHttpClientFactory asyncHttpClientFactory = Mockito.spy(new AsyncHttpClientFactory());
        doReturn(asyncHttpClient).when(asyncHttpClientFactory).createAsyncHttpClient();

        asyncHttpClientFactory.getAsyncHttpClient();
        asyncHttpClientFactory.getAsyncHttpClient();

        verify(asyncHttpClientFactory, times(1)).createAsyncHttpClient();
    }

    @Test
    public void testCreateWithFilter() {
        AsyncHttpClientFactory asyncHttpClientFactory = Mockito.spy(new AsyncHttpClientFactory());
        doReturn(asyncHttpClient).when(asyncHttpClientFactory).createAsyncHttpClientWitHResponseFilter();

        asyncHttpClientFactory.getAsyncHttpClientWitHResponseFilter();
        asyncHttpClientFactory.getAsyncHttpClientWitHResponseFilter();

        verify(asyncHttpClientFactory, times(1)).createAsyncHttpClientWitHResponseFilter();
    }
}