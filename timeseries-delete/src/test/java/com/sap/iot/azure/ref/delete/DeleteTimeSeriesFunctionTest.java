package com.sap.iot.azure.ref.delete;

import com.microsoft.azure.eventhubs.EventDataBatch;
import com.sap.iot.azure.ref.delete.logic.DeleteTimeSeriesHandler;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Map;

import static com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil.createPartitionContext;
import static com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil.createSystemPropertiesMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeleteTimeSeriesFunctionTest {

    @Mock
    private DeleteTimeSeriesHandler deleteHandler;

    @InjectMocks
    DeleteTimeSeriesFunction deleteTimeSeriesFunction;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Captor
    private ArgumentCaptor<Map> systemPropertiesCaptor;

    @Test
    public void testRun() throws IOException {
        String deleteRequest = DeleteTimeSeriesTestUtil.createSimpleDeleteRequest("/DeleteTimeSeriesRequest.json");
        Map<String, Object> test = createSystemPropertiesMap()[0];
        deleteTimeSeriesFunction.run(deleteRequest, test, createPartitionContext(),
                InvocationContextTestUtil.getMockContext());
        verify(deleteHandler, times(1)).processMessage(eq(deleteRequest), systemPropertiesCaptor.capture());
        assertEquals(test.get(CommonConstants.ENQUEUED_TIME_UTC), systemPropertiesCaptor.getValue().get(CommonConstants.ENQUEUED_TIME_UTC));
    }

    @Test
    public void testRunWithException() throws IOException {
        String notificationMessage = DeleteTimeSeriesTestUtil.createSimpleDeleteRequest("/DeleteTimeSeriesRequest.json");
        doThrow(new RuntimeException("test error")).when(deleteHandler).processMessage(anyString(), anyMap());

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("test error");
        deleteTimeSeriesFunction.run(notificationMessage, createSystemPropertiesMap()[0], createPartitionContext(),
                InvocationContextTestUtil.getMockContext());
    }

}