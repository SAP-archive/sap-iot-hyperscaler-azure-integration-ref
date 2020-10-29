package com.sap.iot.azure.ref.delete;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

import static com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil.createPartitionContext;
import static com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil.createSystemPropertiesMap;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeleteTimeSeriesFunctionTest {

    @Mock
    private DeleteTimeSeriesHandler deleteHandler;

    @Spy
    private RetryTaskExecutor retryTaskExecutor;

    @InjectMocks
    DeleteTimeSeriesFunction deleteTimeSeriesFunction;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testRun() throws IOException {
        String deleteRequest = createNotificationMessage("/DeleteTimeSeriesRequest.json");
        deleteTimeSeriesFunction.run(deleteRequest, createSystemPropertiesMap()[0], createPartitionContext(),
                InvocationContextTestUtil.getMockContext());
        verify(deleteHandler, times(1)).processMessage(deleteRequest);
    }

    @Test
    public void testRunWithException() throws IOException {
        String notificationMessage = createNotificationMessage("/DeleteTimeSeriesRequest.json");
        doThrow(new RuntimeException("test error")).when(retryTaskExecutor).executeWithRetry(any(Callable.class), anyInt());

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("test error");
        deleteTimeSeriesFunction.run(notificationMessage, createSystemPropertiesMap()[0], createPartitionContext(),
                InvocationContextTestUtil.getMockContext());
    }

    public String createNotificationMessage(String sampleMessage) throws IOException {
        return IOUtils.toString(this.getClass().getResourceAsStream(sampleMessage), StandardCharsets.UTF_8);
    }
}