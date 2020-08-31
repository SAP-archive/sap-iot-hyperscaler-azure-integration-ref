package com.sap.iot.azure.ref.integration.commons.eventhub;

import com.microsoft.azure.eventhubs.BatchOptions;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.PayloadSizeExceededException;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;
import com.sap.iot.azure.ref.integration.commons.util.CompletableFutures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public abstract class BaseEventHubProcessor<T> {

    private final CompletableFuture<EventHubClient> eventHubCreationFuture;

    protected BaseEventHubProcessor(CompletableFuture<EventHubClient> eventHubCreationFuture) {
        this.eventHubCreationFuture = eventHubCreationFuture;

        eventHubCreationFuture.whenCompleteAsync((eventHubClientAlias, ex) -> {
            if (ex != null) {
                // exception will be thrown when sending message
                InvocationContext.getLogger().log(Level.SEVERE, "Error in initializing Event Hub Connection", ex);
            }
        });
    }

    /**
     * convert the {@link ProcessedMessageContainer} object to the respective processor format (ADX / Avro)
     * @param message   message to be sent to Event Hub
     * @return list of event data
     */
    protected abstract List<EventData> createEventData(T message);

    /**
     * batches the messages and sends to EventHUb
     * @param message       message to be sent to Event Hub
     * @param partitionKey  partition key
     * @return CompletableFuture for sending messages to EventHub
     */
    protected CompletableFuture<Void> process(T message, String partitionKey) {

        EventHubClient eventHubClient;
        try {
            // only the first invocation of get - there's a sync wait involved; for subsequent calls, the CF is already completed
            eventHubClient = eventHubCreationFuture.get();
        } catch (InterruptedException e) {
            // set the interrupted state again
            Thread.currentThread().interrupt();
            throw IoTRuntimeException.wrapNonTransient(IdentifierUtil.getIdentifier("PartitionKey", partitionKey), CommonErrorType.EVENT_HUB_ERROR,
                    "Interrupted while waiting for EventHub Connection Initialization", e); // will not retry since it's interrupted
        } catch (ExecutionException e) {
            throw new IoTRuntimeException("Error in initializing Event Hub Connection", CommonErrorType.EVENT_HUB_ERROR,
                    InvocationContext.getContext().getInvocationId(), IdentifierUtil.getIdentifier("PartitionKey", partitionKey), isTransient(e));
        }

        List<EventData> eventDataList = this.createEventData(message);
        if (eventDataList.isEmpty()) {
            return CompletableFutures.voidCompletedFuture();
        } else {

            List<CompletableFuture<Void>> sendFutures = new ArrayList<>();
            int batchCounter = 0;
            BatchOptions batchOptions = new BatchOptions().with(options -> {
                // max message will be automatically determined by the event hub client if not set using internal MessageSender#getMaxMessageSize() method
                options.partitionKey = partitionKey;
            });

            EventDataBatch eventDataBatch;
            try {
                eventDataBatch = eventHubClient.createBatch(batchOptions);
            } catch (EventHubException e) {
                // not expected - since the size is nt explicitly set for the batch
                throw IoTRuntimeException.wrapNonTransient(IdentifierUtil.empty(), CommonErrorType.EVENT_HUB_ERROR, "Error in creating Event Hub Batch", e);
            }

            for (int i = 0; i < eventDataList.size();) {
                EventData eventData = eventDataList.get(i);

                try {
                    if (eventData != null && eventDataBatch.tryAdd(eventData)) {
                        // in case adding eventData is successful, we'll proceed to the next event data; In case add fails, the counter is not incremented
                        i++;
                    } else {

                        // send the current batch (until last event Data and prepare the next batch
                        eventDataBatch = sendCurrentBatchAndPrepareNextBatch(eventHubClient, eventDataBatch, partitionKey, sendFutures, batchOptions, batchCounter++);
                    }
                } catch (PayloadSizeExceededException ex) {

                    if (eventDataBatch.getSize() == 0) {
                        // single event data is more than allowed size - unexpected; will be treated as permanent exception
                        throw IoTRuntimeException.wrapNonTransient(IdentifierUtil.getIdentifier("Partition Key", partitionKey), CommonErrorType.EVENT_HUB_ERROR,
                                "Event Data is greater than allowed size", ex);
                    }

                    // tryAdd method can still throw PayloadSizeExceededException - in which case we send previous batch and start with preparing next batch
                    eventDataBatch = sendCurrentBatchAndPrepareNextBatch(eventHubClient, eventDataBatch, partitionKey, sendFutures, batchOptions, batchCounter++);
                }
            }

            // send the last batch - if any existing; we can skip the next EventDataBatch created
            if (eventDataBatch != null && eventDataBatch.getSize() >= 0) {
                sendCurrentBatchAndPrepareNextBatch(eventHubClient, eventDataBatch, partitionKey, sendFutures, batchOptions, batchCounter);
            }

            return CompletableFuture.allOf(sendFutures.toArray(new CompletableFuture[0]));
        }
    }

    private EventDataBatch sendCurrentBatchAndPrepareNextBatch(EventHubClient eventHubClient, EventDataBatch eventDataBatch, String partitionKey,
                                                               List<CompletableFuture<Void>> sendFutures, BatchOptions batchOptions, int batchCounter) throws IoTRuntimeException {

        InvocationContext.getLogger().fine(String.format("Sending event hub batch message %d with size %d", batchCounter, eventDataBatch.getSize()));
        sendFutures.add(eventHubClient.send(eventDataBatch)
                .exceptionally(ex -> {
                    throw new IoTRuntimeException("error in sending event data batch", CommonErrorType.EVENT_HUB_ERROR,
                            InvocationContext.getContext().getInvocationId(), IdentifierUtil.getIdentifier("Partition Key", partitionKey), isTransient(ex));
                }));

        try {
            return eventHubClient.createBatch(batchOptions);
        } catch (EventHubException ex) {

            // this is not expected since the batch size not set explicitly, but requires handling as checked exception
            throw new IoTRuntimeException("Error in creating Event Hub Batch", CommonErrorType.EVENT_HUB_ERROR,
                    InvocationContext.getContext().getInvocationId(), IdentifierUtil.getIdentifier("Partition Key", partitionKey), isTransient(ex));
        }
    }

    private boolean isTransient(Throwable ex) {
        boolean isTransient = true;
        if (ex instanceof ExecutionException && EventHubException.class.isAssignableFrom(ex.getCause().getClass())) {
            isTransient = ((EventHubException) ex.getCause()).getIsTransient();
        } else if (ex instanceof EventHubException) {
            isTransient = ((EventHubException) ex).getIsTransient();
        }
        return isTransient;
    }
}
