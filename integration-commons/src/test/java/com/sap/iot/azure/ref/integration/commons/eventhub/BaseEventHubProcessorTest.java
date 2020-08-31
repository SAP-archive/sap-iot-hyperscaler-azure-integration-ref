package com.sap.iot.azure.ref.integration.commons.eventhub;

import com.microsoft.azure.eventhubs.BatchOptions;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.PayloadSizeExceededException;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;
import com.sap.iot.azure.ref.integration.commons.util.CompletableFutures;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BaseEventHubProcessorTest {

    @Mock
    private EventHubClient eventHubClient;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void classSetup() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Before
    public void setup() throws EventHubException {
        when(eventHubClient.createBatch(any(BatchOptions.class))).thenReturn(new SimpleEventBatch(), new SimpleEventBatch());
        when(eventHubClient.send(any(EventDataBatch.class))).thenReturn(CompletableFutures.voidCompletedFuture());
    }

    @Test
    public void testSend() throws ExecutionException, InterruptedException {
        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(eventHubClient, 1, 100);
        //Test implementation will return a list of EventData
        eventHubProcessor.process(new ProcessedMessageContainer(new ArrayList<>()), "").get();

        //Since EventData is present, send should be invoked
        verify(eventHubClient, times(1)).send(any(EventDataBatch.class));
    }

    @Test
    public void testEmptyList() throws ExecutionException, InterruptedException {
        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(eventHubClient, 0, 0);
        //Test implementation will return an empty list of EventData
        eventHubProcessor.process(new ProcessedMessageContainer(new ArrayList<>()), "").get();

        //Since no EventData is present, send should not be invoked
        verify(eventHubClient, times(0)).send(any(EventDataBatch.class));
    }

    @Test
    public void testSendMultiBatch() throws ExecutionException, InterruptedException {
        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(eventHubClient, 15, 100);

        //Test implementation will return a list of EventData
        eventHubProcessor.process(new ProcessedMessageContainer(), "").get();

        // sending 15 messages (100 bytes per message) --> since the max size is 1000 bytes, this should yield 2 batches
        verify(eventHubClient, times(2)).send(any(EventDataBatch.class));
    }

    @Test
    public void testSendMultiBatchWithNoTailBatch() throws ExecutionException, InterruptedException {
        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(eventHubClient, 20, 100);

        //Test implementation will return a list of EventData
        eventHubProcessor.process(new ProcessedMessageContainer(), "").get();

        // sending 15 messages (100 bytes per message) --> since the max size is 1000 bytes, this should yield 2 batches; ensure that no additional empty
        // EventBatch is generated & sent
        verify(eventHubClient, times(2)).send(any(EventDataBatch.class));
    }

    @Test
    public void testBaseEventHubWithTransientFailedEventHubConnection() throws ExecutionException, InterruptedException {
        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(0, 0, true);

        thrown.expect(IoTRuntimeException.class);
        thrown.expectMessage("EVENT_HUB_ERROR");
        thrown.expectMessage("Error in initializing Event Hub Connection");
        thrown.expectMessage("\"Transient\":true");
        eventHubProcessor.process(new ProcessedMessageContainer(), "").get();
    }

    @Test
    public void testBaseEventHubWithPermanentFailedEventHubConnection() throws ExecutionException, InterruptedException {
        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(0, 0, false);

        thrown.expect(IoTRuntimeException.class);
        thrown.expectMessage("EVENT_HUB_ERROR");
        thrown.expectMessage("Error in initializing Event Hub Connection");
        thrown.expectMessage("\"Transient\":false");
        eventHubProcessor.process(new ProcessedMessageContainer(), "").get();
    }

    @Test
    public void testSendingTooLargeMessage() throws ExecutionException, InterruptedException { // single message greater than allowed size limit
        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(eventHubClient, 1, 1001);

        thrown.expect(IoTRuntimeException.class);
        thrown.expectMessage("EVENT_HUB_ERROR");
        thrown.expectMessage("Event Data is greater than allowed size");
        thrown.expectMessage("\"Transient\":false");
        eventHubProcessor.process(new ProcessedMessageContainer(), "").get();
    }

    @Test
    public void testBatchCreationException() throws EventHubException, ExecutionException, InterruptedException {
        reset(eventHubClient); // clears up the stub to return EventBatch
        doThrow(new EventHubException(false, "create batch failed")).when(eventHubClient).createBatch(any(BatchOptions.class));

        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(eventHubClient, 1, 100);

        thrown.expect(IoTRuntimeException.class);
        thrown.expectMessage("EVENT_HUB_ERROR");
        thrown.expectMessage("Error in creating Event Hub Batch");
        thrown.expectMessage("\"Transient\":false");
        eventHubProcessor.process(new ProcessedMessageContainer(), "").get();
    }

    @Test
    public void testSubsequentBatchCreationException() throws EventHubException, ExecutionException, InterruptedException {
        reset(eventHubClient); // clears up the stub to return EventBatch

        // first batch is created successfully, & then throw batch creation exception
        when(eventHubClient.createBatch(any(BatchOptions.class))).thenReturn(new SimpleEventBatch()).thenThrow(new EventHubException(false, "create batch failed"));
        when(eventHubClient.send(any(EventDataBatch.class))).thenReturn(CompletableFutures.voidCompletedFuture());

        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(eventHubClient, 12, 100);

        thrown.expect(IoTRuntimeException.class);
        thrown.expectMessage("EVENT_HUB_ERROR");
        thrown.expectMessage("Error in creating Event Hub Batch");
        thrown.expectMessage("\"Transient\":false");
        eventHubProcessor.process(new ProcessedMessageContainer(), "").get();
    }

    @Test
    public void testEventHubSendError() throws EventHubException, ExecutionException, InterruptedException {
        reset(eventHubClient); // clears up the stub to return EventBatch

        when(eventHubClient.createBatch(any(BatchOptions.class))).thenReturn(new SimpleEventBatch());
        when(eventHubClient.send(any(EventDataBatch.class))).thenReturn(CompletableFutures.completeExceptionally(new EventHubException(false,"error")));

        EventHubProcessorImpl eventHubProcessor = new EventHubProcessorImpl(eventHubClient, 2, 100);

        thrown.expect(ExecutionException.class);
        thrown.expectMessage("error in sending event data batch");
        thrown.expectMessage("EVENT_HUB_ERROR");
        thrown.expectMessage("\"Transient\":false");
        eventHubProcessor.process(new ProcessedMessageContainer(), "").get();
    }

    static class EventHubProcessorImpl extends BaseEventHubProcessor<ProcessedMessageContainer> {
        private final int numberOfEvents;
        private final int messageSize;

        EventHubProcessorImpl(EventHubClient eventHubClient, int numberOfEvents, int messageSize) {
            super(CompletableFuture.completedFuture(eventHubClient));
            this.numberOfEvents = numberOfEvents;
            this.messageSize = messageSize; // in bytes
        }

        EventHubProcessorImpl(int numberOfEvents, int messageSize, boolean failedTransiently) {
            super(CompletableFutures.completeExceptionally(new EventHubException(failedTransiently, "test error")));
            this.numberOfEvents = numberOfEvents;
            this.messageSize = messageSize; // in bytes
        }

        @Override
        protected List<EventData> createEventData(ProcessedMessageContainer processedMessageContainer) {
            List<EventData> eventDataList = new ArrayList<>();
            for (int i = 0; i < numberOfEvents; i++) {

                byte[] data = new byte[messageSize];
                Arrays.fill(data, "a".getBytes()[0]);

                eventDataList.add(EventData.create(data));
            }

            return eventDataList;
        }
    }

    public static class SimpleEventBatch implements EventDataBatch {

        private int batchCount = 0;
        private long batchSize = 0;
        private List<byte[]> batchData = new ArrayList<>();
        private List<Map<String, Object>> batchProperties = new ArrayList<>();

        @Override
        public int getSize() {
            return batchCount;
        }

        @Override
        public boolean tryAdd(EventData eventData) throws PayloadSizeExceededException {

            // in actual event hub allows 256 KB (basic) or 1MB (standard) tier
            int maxSize = 1000;

            if (batchSize + eventData.getBytes().length <= maxSize) {
                batchData.add(eventData.getBytes());
                batchProperties.add(eventData.getProperties());

                batchSize += eventData.getBytes().length;
                batchCount++;
                return true;
            } else if (batchCount == 0) {
                throw new PayloadSizeExceededException("Message size too large");
            } else {
                return false;
            }
        }

        public byte[] getBytes(int index) {
            return batchData.get(index);
        }

        public Map<String, Object> getProperties(int index) {
            return batchProperties.get(index);
        }
    }
}