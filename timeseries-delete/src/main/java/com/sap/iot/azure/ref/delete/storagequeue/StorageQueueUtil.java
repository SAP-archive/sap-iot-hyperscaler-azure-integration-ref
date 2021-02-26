package com.sap.iot.azure.ref.delete.storagequeue;

import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.queue.QueueRequestOptions;
import com.sap.iot.azure.ref.delete.util.Constants;

public class StorageQueueUtil {
    /**
     * Builds a {@link QueueRequestOptions} object containing retry configuration with exponential backoff.
     * The configuration is defined through the Constants {@link Constants#STORAGE_QUEUE_MIN_BACKOFF}, {@link Constants#STORAGE_QUEUE_DELTA_BACKOFF},
     * {@link Constants#STORAGE_QUEUE_MAX_BACKOFF} and {@link Constants#STORAGE_QUEUE_MAX_RETRIES}.
     *
     * @return {@link QueueRequestOptions} object containing retry configuration
     */
    public static QueueRequestOptions getRetryOptions() {
        RetryExponentialRetry retryExponentialRetry = new RetryExponentialRetry(Constants.STORAGE_QUEUE_MIN_BACKOFF,
                Constants.STORAGE_QUEUE_DELTA_BACKOFF, Constants.STORAGE_QUEUE_MAX_BACKOFF, Constants.STORAGE_QUEUE_MAX_RETRIES);
        QueueRequestOptions queueRequestOptions = new QueueRequestOptions();
        queueRequestOptions.setRetryPolicyFactory(retryExponentialRetry);
        return queueRequestOptions;
    }

}
