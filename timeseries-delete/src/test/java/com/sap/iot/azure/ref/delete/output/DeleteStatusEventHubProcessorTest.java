package com.sap.iot.azure.ref.delete.output;

import com.microsoft.azure.eventhubs.BatchOptions;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.sap.iot.azure.ref.delete.model.DeleteStatusMessage;
import com.sap.iot.azure.ref.delete.model.DeleteStatus;
import com.sap.iot.azure.ref.integration.commons.eventhub.BaseEventHubProcessorTest;
import com.sap.iot.azure.ref.integration.commons.util.CompletableFutures;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeleteStatusEventHubProcessorTest {

    DeleteStatusEventHubProcessor deleteStatusEventHubProcessor;
    @Mock
    EventHubClient ehClientMock;
    @Before
    public void setup() throws EventHubException {
        deleteStatusEventHubProcessor = Mockito.spy(new DeleteStatusEventHubProcessor(CompletableFuture.completedFuture(ehClientMock)));//spying the actual
        // class to verify interaction on base class
        when(ehClientMock.createBatch(any(BatchOptions.class))).thenReturn(new BaseEventHubProcessorTest.SimpleEventBatch());
        when(ehClientMock.send(any(EventDataBatch.class))).thenReturn(CompletableFutures.voidCompletedFuture());
    }

    @Test
    public void testProcess(){
        DeleteStatusMessage deleteStatusMessage = new DeleteStatusMessage();
        deleteStatusMessage.setStatus(DeleteStatus.SUCCESS);
        deleteStatusMessage.setEventId("testEventId");
        deleteStatusMessage.setStructureId("testStructureId");
        deleteStatusMessage.setCorrelationId("testCorrelationId");
        deleteStatusEventHubProcessor.process(deleteStatusMessage);
        verify(deleteStatusEventHubProcessor, times(1)).createEventData(deleteStatusMessage);
        verify(deleteStatusEventHubProcessor, times(1)).convertToEventData(deleteStatusMessage);
    }
}