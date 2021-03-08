package com.sap.iot.azure.ref.delete.storagequeue;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.connection.CloudQueueClientFactory;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;

import java.net.URISyntaxException;

public class PurgeStorageQueueProcessor implements Processor<CloudQueueMessage, Void> {
    private final CloudQueueClient cloudQueueClient;

    public PurgeStorageQueueProcessor() {
        this(new CloudQueueClientFactory().getCloudQueueClient());
    }

    PurgeStorageQueueProcessor(CloudQueueClient cloudQueueClient) {
        this.cloudQueueClient = cloudQueueClient;
    }

    /**
     * Sends {@link CloudQueueMessage} to the purge storage queue.
     * The purge storage queue name is defined in {@link Constants#PURGE_QUEUE_NAME}.
     *
     * @param message message to be sent to purge storage queue
     * @throws DeleteTimeSeriesException error while interacting with storage queue
     */
    public Void process(CloudQueueMessage message) throws DeleteTimeSeriesException {
        try {
            // Retrieve a reference to a queue.
            CloudQueue queue = cloudQueueClient.getQueueReference(Constants.PURGE_QUEUE_NAME);
            // Create the queue if it doesn't already exist.
            queue.createIfNotExists();
            queue.addMessage(message, 0, 0, StorageQueueUtil.getRetryOptions(), null);
        } catch (StorageException e) {
            throw new DeleteTimeSeriesException(String.format("Error while adding cloudQueueMessage to queue after %s retries. HTTPStatusCode: %s and Error " +
                    "Code: %s", Constants.STORAGE_QUEUE_MAX_RETRIES, e.getHttpStatusCode(), e.getErrorCode()), e,
                    DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR, IdentifierUtil.getIdentifier(Constants.MESSAGE_ID_KEY, message.getId()), false);
        } catch (URISyntaxException e) {
            throw new DeleteTimeSeriesException("Invalid Storage Queue Address", e, DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR,
                    IdentifierUtil.getIdentifier(Constants.MESSAGE_ID_KEY, message.getId()), false);
        }

        return null; //required to implement the processor interface
    }
}
