package com.sap.iot.azure.ref.delete;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.connection.CloudQueueClientFactory;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.exception.StorageExceptionUtil;
import com.sap.iot.azure.ref.delete.logic.PurgeTimeSeriesHandler;
import com.sap.iot.azure.ref.delete.storagequeue.StorageQueueUtil;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.util.EnvUtils;

import java.util.List;
import java.util.logging.Level;

public class BatchPurgeTimeSeriesFunction {

    private final PurgeTimeSeriesHandler purgeTimeSeriesHandler;
    private CloudQueue purgeQueue;

    public BatchPurgeTimeSeriesFunction() {
        this(new PurgeTimeSeriesHandler(), new CloudQueueClientFactory().getPurgeQueue());
    }

    @VisibleForTesting
    BatchPurgeTimeSeriesFunction(PurgeTimeSeriesHandler purgeTimeSeriesHandler, CloudQueue purgeQueue) {
        this.purgeTimeSeriesHandler = purgeTimeSeriesHandler;
        this.purgeQueue = purgeQueue;
    }

    /**
     * Azure Function with timer trigger that is responsible for batching and executing the purge time series requests.
     * In a loop, the function fetches the maximum amount ({@link Constants#MAX_PURGE_REQUESTS_DEFAULT}) of messages from the batch request storage queue and processes them.
     * The processing is done by {@link PurgeTimeSeriesHandler}.
     *
     * @param context, invocation context of the current Azure Function invocation
     */
    @FunctionName("BatchPurgeTimeSeries")
    public void run(
            @TimerTrigger(name = "timedPurgeTrigger", schedule = "0 0 */12 * * *") String timerInfo,
            ExecutionContext context
    ) {
        int totalMessageCount = 0;
        int purgeMessageCount;


        InvocationContext.setupInvocationContext(context);
        do {
            List<CloudQueueMessage> purgeMessages = fetchPurgeMessages();
            purgeMessageCount = purgeMessages.size();
            purgeTimeSeriesHandler.processMessages(purgeMessages);
            totalMessageCount += purgeMessageCount;
            InvocationContext.getLogger().log(Level.INFO, String.format("%s purge messages processed", purgeMessageCount));
        } while (purgeMessageCount > 0);

        InvocationContext.getLogger().log(Level.INFO, String.format("A total of %s purge messages processed", totalMessageCount));
    }

    private List<CloudQueueMessage> fetchPurgeMessages() throws DeleteTimeSeriesException {
        try {
            Iterable<CloudQueueMessage> purgeMessages = purgeQueue.retrieveMessages(EnvUtils.getEnv(Constants.MAX_PURGE_REQUESTS_NAME,
                    Constants.MAX_PURGE_REQUESTS_DEFAULT), Constants.DEFAULT_VISIBILITY_TIMEOUT_IN_SECONDS,
                    StorageQueueUtil.getRetryOptions(), null);

            return Lists.newArrayList(purgeMessages);
        } catch (StorageException e) {
            throw new DeleteTimeSeriesException(String.format("Unable to fetch purge messages after %s retries", Constants.MAX_RETRIES), e, DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR,
                    IdentifierUtil.empty(), StorageExceptionUtil.isTransient(e));
        }
    }
}
