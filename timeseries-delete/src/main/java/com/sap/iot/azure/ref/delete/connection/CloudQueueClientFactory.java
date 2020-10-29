package com.sap.iot.azure.ref.delete.connection;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.CloudStorageAccount;
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

    public synchronized CloudQueueClient getCloudQueueClient() {
        MetricsClient.timed(() -> {
            if (cloudQueueClient == null) {
                try {
                    cloudQueueClient = createCloudQueueClient();
                } catch (URISyntaxException | InvalidKeyException e) {
                    throw new DeleteTimeSeriesException("Invalid connection string provided for IoT Hub", e, DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR,
                            IdentifierUtil.empty(), false);
                }
            }
        }, "CloudQueueInit");

        return cloudQueueClient;
    }

    @VisibleForTesting
    CloudQueueClient createCloudQueueClient() throws URISyntaxException, InvalidKeyException {
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(System.getenv(Constants.STORAGE_CONNECTION_STRING_PROP));

        return storageAccount.createCloudQueueClient();
    }


}
