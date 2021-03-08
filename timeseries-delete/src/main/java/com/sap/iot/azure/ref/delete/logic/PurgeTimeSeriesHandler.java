package com.sap.iot.azure.ref.delete.logic;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.connection.CloudQueueClientFactory;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.model.OperationType;
import com.sap.iot.azure.ref.delete.model.PurgeInfo;
import com.sap.iot.azure.ref.delete.model.cloudEvents.SapIoTAbstractionExtension;
import com.sap.iot.azure.ref.delete.storagequeue.OperationStorageQueueProcessor;
import com.sap.iot.azure.ref.delete.storagequeue.StorageQueueMessageInfo;
import com.sap.iot.azure.ref.delete.storagequeue.StorageQueueUtil;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.delete.util.StatusQueueHelper;
import com.sap.iot.azure.ref.integration.commons.adx.ADXDataManager;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;
import io.cloudevents.Attributes;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.logging.Level;

public class PurgeTimeSeriesHandler {

    private final ADXDataManager adxDataManager;
    private final OperationStorageQueueProcessor operationStorageQueueProcessor;
    private final RetryTaskExecutor retryTaskExecutor;
    private final CloudQueue purgeQueue;
    private final StatusQueueHelper statusQueueHelper;

    public PurgeTimeSeriesHandler() {
        this(new ADXDataManager(), new OperationStorageQueueProcessor(), new CloudQueueClientFactory().getPurgeQueue(), new RetryTaskExecutor(),
                new StatusQueueHelper());
    }

    @VisibleForTesting
    PurgeTimeSeriesHandler(ADXDataManager adxDataManager, OperationStorageQueueProcessor operationStorageQueueProcessor, CloudQueue purgeQueue,
                           RetryTaskExecutor retryTaskExecutor, StatusQueueHelper statusQueueHelper) {
        this.adxDataManager = adxDataManager;
        this.operationStorageQueueProcessor = operationStorageQueueProcessor;
        this.purgeQueue = purgeQueue;
        this.retryTaskExecutor = retryTaskExecutor;
        this.statusQueueHelper = statusQueueHelper;
    }

    /**
     * Processes a list of purge messages.
     * The purge requests are first grouped by structure id.
     * For every group, a purge query is formed and executed using.
     * The purge query execution is wrapped with a retry logic using the {@link RetryTaskExecutor}.
     * If the purge query is successful, all requests of the group are dequeued from the storage queue.
     * If the purge query fails, the status is sent to the delete time series status queue.
     *
     * @param purgeMessages list of purge messages
     * @throws DeleteTimeSeriesException exception while parsing message
     */
    public void processMessages(List<CloudQueueMessage> purgeMessages) {
        try {
            Map<String, PurgeInfo> purgeRequestsByStructure = groupPurgeMessages(purgeMessages);
            handlePurge(purgeRequestsByStructure);
        } catch (IllegalStateException e) {
            throw new DeleteTimeSeriesException("Unable to parse message", e, DeleteTimeSeriesErrorType.INVALID_MESSAGE,
                    IdentifierUtil.empty(), false);
        }
    }

    private Map<String, PurgeInfo> groupPurgeMessages(List<CloudQueueMessage> purgeMessages) throws DeleteTimeSeriesException {
        Map<String, PurgeInfo> requestsByStructureId = new HashMap<>();

        for (CloudQueueMessage purgeMessage : purgeMessages) {
            try {
                CloudEvent<Attributes, DeleteInfo> cloudEvent = Json.decodeValue(purgeMessage.getMessageContentAsString(), CloudEventImpl.class, DeleteInfo.class);

                try {
                    DeleteInfo deleteInfo = getDeleteInfo(cloudEvent);
                    String structureId = deleteInfo.getStructureId();
                    PurgeInfo purgeInfo = requestsByStructureId.get(structureId);

                    if (purgeInfo == null) {
                        purgeInfo = PurgeInfo.builder().cloudQueueMessages(new ArrayList<>()).deleteInfos(new ArrayList<>()).build();
                        requestsByStructureId.put(structureId, purgeInfo);
                    }
                    purgeInfo.getCloudQueueMessages().add(purgeMessage);
                    purgeInfo.getDeleteInfos().add(deleteInfo);
                } catch (DeleteTimeSeriesException e) {
                    InvocationContext.getLogger().severe("Unable to get delete info from delete request.");
                    statusQueueHelper.sendFailedDeleteToStatusQueue(cloudEvent.getAttributes().getId());
                }
            } catch (IllegalStateException e) {
                //todo: Log exception with json format
                InvocationContext.getLogger().log(Level.SEVERE, String.format("Unable to convert purge message with id \"%s\" to cloud event.",
                        purgeMessage.getId()), e);
            } catch (StorageException e) {
                throw new DeleteTimeSeriesException("Unable to get content of purge message", e,
                        DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR, IdentifierUtil.getIdentifier(CommonConstants.MESSAGE_ID, purgeMessage.getId()), false);
            }
        }

        return requestsByStructureId;
    }

    private DeleteInfo getDeleteInfo(CloudEvent<Attributes, DeleteInfo> cloudEvent) throws DeleteTimeSeriesException {
        DeleteInfo deleteInfo = cloudEvent.getData().orElseThrow(() -> new DeleteTimeSeriesException("Delete request info is missing in the message",
                DeleteTimeSeriesErrorType.INVALID_MESSAGE, IdentifierUtil.getIdentifier("CloudEventId", cloudEvent.getAttributes().getId()), false));
        deleteInfo.setEventId(cloudEvent.getAttributes().getId());
        deleteInfo.setCorrelationId(SapIoTAbstractionExtension.getExtension(cloudEvent).getCorrelationId());

        return deleteInfo;
    }

    private void handlePurge(Map<String, PurgeInfo> purgeRequestsByStructure) {
        purgeRequestsByStructure.entrySet().forEach(purgeEntry -> {
            PurgeInfo purgeInfo = purgeEntry.getValue();
            try {
                String operationId = retryTaskExecutor.executeWithRetry(() ->
                                CompletableFuture.supplyAsync(InvocationContext.withContext((Supplier<String>) () ->
                                        executePurgeQuery(purgeEntry.getKey(), purgeInfo))),
                        Constants.MAX_RETRIES).join();
                purgeInfo.getDeleteInfos().forEach(deleteInfo -> {
                    CloudQueueMessage storageQueueMessage = operationStorageQueueProcessor.getOperationInfoMessage(operationId,
                            deleteInfo.getEventId(), deleteInfo.getStructureId(), deleteInfo.getCorrelationId(), OperationType.PURGE);

                    operationStorageQueueProcessor.apply(new StorageQueueMessageInfo(storageQueueMessage, Optional.empty()));
                });

                dequeuePurgeMessages(purgeInfo.getCloudQueueMessages());
            } catch (ADXClientException e) {
                purgeInfo.getDeleteInfos().forEach(deleteInfo -> {
                    statusQueueHelper.sendFailedDeleteToStatusQueue(deleteInfo, false);
                });
                throw new DeleteTimeSeriesException(String.format("Unable to execute purge queue after %s retries. Sending failed status to status queue for " +
                        "affected messages.", Constants.MAX_RETRIES), e, DeleteTimeSeriesErrorType.ADX_DATA_QUERY_EXCEPTION, IdentifierUtil.empty(), false);
            }
        });
    }

    private String executePurgeQuery(String structureId, PurgeInfo purgeInfo) throws ADXClientException {
        return adxDataManager.purgeTimeSeries(structureId, purgeInfo.getDeleteInfos());
    }

    private void dequeuePurgeMessages(List<CloudQueueMessage> messages) throws DeleteTimeSeriesException {
        for (CloudQueueMessage message : messages) {
            try {
                purgeQueue.deleteMessage(message, StorageQueueUtil.getRetryOptions(), null);
            } catch (StorageException e) {
                InvocationContext.getLogger().log(Level.SEVERE, new DeleteTimeSeriesException(String.format("Unable to dequeue purge messages after %s retries",
                        Constants.MAX_RETRIES), e, DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR, IdentifierUtil.getIdentifier(CommonConstants.MESSAGE_ID,
                        message.getId()), false).jsonify().toString(), e);
            }
        }
    }
}
