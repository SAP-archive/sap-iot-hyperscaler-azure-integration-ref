package com.sap.iot.azure.ref.delete.connection;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.metrics.MetricsClient;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class CloudQueueClientFactory {
    private static CloudQueueClient cloudQueueClient;
    private static CloudQueue purgeQueue;

    /**
     * Create an {@link CloudQueueClient}.
     * A {@link DeleteTimeSeriesException} is thrown in case of failure.
     *
     * @return {@link CloudQueueClient}
     * @throws DeleteTimeSeriesException exception in cloud queue client creation
     */
    public synchronized CloudQueueClient getCloudQueueClient() throws DeleteTimeSeriesException {
        MetricsClient.timed(() -> {
            if (cloudQueueClient == null) {
                try {
                    cloudQueueClient = createCloudQueueClient();
                } catch (URISyntaxException | InvalidKeyException e) {
                    throw new DeleteTimeSeriesException("Invalid connection string provided for storage account", e,
                            DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR, IdentifierUtil.empty(), false);
                }
            }
        }, "CloudQueueClientInit");

        return cloudQueueClient;
    }

    /**
     * Create an {@link CloudQueue} for the queue name defined in {@link Constants#PURGE_QUEUE_NAME}.
     * A {@link DeleteTimeSeriesException} is thrown in case of failure.
     *
     * @return {@link CloudQueue} purge queue
     * @throws DeleteTimeSeriesException exception in cloud queue creation
     */
    public synchronized CloudQueue getPurgeQueue() throws DeleteTimeSeriesException {
        MetricsClient.timed(() -> {
            if (purgeQueue == null) {
                try {
                    purgeQueue = getCloudQueueClient().getQueueReference(Constants.PURGE_QUEUE_NAME);
                } catch (URISyntaxException | StorageException e) {
                    throw new DeleteTimeSeriesException("Unable to instantiate purge queue", e, DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR,
                            IdentifierUtil.empty(), false);
                }
            }
        }, "CloudQueueInit");

        return purgeQueue;
    }

    @VisibleForTesting
    CloudQueueClient createCloudQueueClient() throws URISyntaxException, InvalidKeyException {
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(System.getenv(Constants.STORAGE_CONNECTION_STRING_PROP));

        return storageAccount.createCloudQueueClient();
    }

    @VisibleForTesting
    synchronized void initializeClass() {
        cloudQueueClient = null;
        purgeQueue = null;
    }

}
