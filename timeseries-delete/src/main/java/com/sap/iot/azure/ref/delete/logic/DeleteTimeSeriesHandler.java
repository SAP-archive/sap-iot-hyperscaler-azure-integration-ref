package com.sap.iot.azure.ref.delete.logic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.exception.InvalidMessageException;
import com.sap.iot.azure.ref.delete.model.OperationType;
import com.sap.iot.azure.ref.delete.model.cloudEvents.SapIoTAbstractionExtension;
import com.sap.iot.azure.ref.delete.storagequeue.OperationStorageQueueProcessor;
import com.sap.iot.azure.ref.delete.storagequeue.PurgeStorageQueueProcessor;
import com.sap.iot.azure.ref.delete.storagequeue.StorageQueueMessageInfo;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.delete.util.StatusQueueHelper;
import com.sap.iot.azure.ref.integration.commons.adx.ADXDataManager;
import com.sap.iot.azure.ref.integration.commons.avro.AvroHelper;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingHelper;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import com.sap.iot.azure.ref.integration.commons.util.EnvUtils;
import io.cloudevents.Attributes;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;


public class DeleteTimeSeriesHandler {

    private final ADXDataManager adxDataManager;
    private final OperationStorageQueueProcessor operationStorageQueueProcessor;
    private final PurgeStorageQueueProcessor purgeStorageQueueProcessor;
    private final MappingHelper mappingHelper;
    private final StatusQueueHelper statusQueueHelper;
    private static final ObjectMapper mapper = new ObjectMapper();

    public DeleteTimeSeriesHandler() {
        this(new ADXDataManager(), new OperationStorageQueueProcessor(), new PurgeStorageQueueProcessor(), new MappingHelper(), new StatusQueueHelper());
    }

    @VisibleForTesting
    DeleteTimeSeriesHandler(ADXDataManager adxDataManager, OperationStorageQueueProcessor operationStorageQueueProcessor,
                            PurgeStorageQueueProcessor purgeStorageQueueProcessor, MappingHelper mappingHelper, StatusQueueHelper statusQueueHelper) {
        this.adxDataManager = adxDataManager;
        this.operationStorageQueueProcessor = operationStorageQueueProcessor;
        this.purgeStorageQueueProcessor = purgeStorageQueueProcessor;
        this.mappingHelper = mappingHelper;
        this.statusQueueHelper = statusQueueHelper;
    }

    /**
     * Processes a delete time series message.
     * The message is expected to be {@link CloudEvent}. The data should be compatible to the {@link DeleteInfo} class.
     * Depending on the gdpr data category of the affected structure, the delete request is either executed as soft delete or purge.
     * In the case of a purge execution, it is either executed immediately or forwarded to the purge storage queue. This behaviour is configured via
     * {@link Constants#IMMEDIATE_PURGE_EXECUTION_NAME}. The default value is false.
     *
     * @param message contains delete request
     * @throws DeleteTimeSeriesException exception while parsing message
     */
    public void processMessage(String message, Map<String, Object> systemProperties) {
        JsonNode systemPropertiesJson = mapper.convertValue(systemProperties, JsonNode.class);
        try {
            CloudEvent<Attributes, DeleteInfo> deleteRequest = Json.decodeValue(message, CloudEventImpl.class, DeleteInfo.class);
            try {
                DeleteInfo deleteInfo = getDeleteInfoFromCloudEvent(deleteRequest, systemProperties);
                if (AvroHelper.isGdprRelevant(mappingHelper.getSchemaInfo(deleteInfo.getStructureId()))) {
                    if (EnvUtils.getEnv(Constants.IMMEDIATE_PURGE_EXECUTION_NAME, false)) {
                        handlePurge(deleteInfo);
                    } else {
                        purgeStorageQueueProcessor.apply(new CloudQueueMessage(message));
                    }
                } else {
                    handleSoftDelete(deleteInfo);
                }
            } catch (InvalidMessageException e) {
                InvocationContext.getLogger().severe("Unable to get delete info from delete request.");
                statusQueueHelper.sendFailedDeleteToStatusQueue(deleteRequest.getAttributes().getId());
            }
        } catch (IllegalStateException e) {
            throw new DeleteTimeSeriesException("Unable to parse message", e, DeleteTimeSeriesErrorType.INVALID_MESSAGE,
                    systemPropertiesJson, false);
        }
    }

    private DeleteInfo getDeleteInfoFromCloudEvent(CloudEvent<Attributes, DeleteInfo> deleteRequest, Map<String, Object> systemProperties) throws DeleteTimeSeriesException {
        DeleteInfo deleteInfo = deleteRequest.getData().orElseThrow(() -> new InvalidMessageException("Delete request info is missing in the message",
                DeleteTimeSeriesErrorType.INVALID_MESSAGE, IdentifierUtil.getIdentifier("CloudEventId", deleteRequest.getAttributes().getId()), false));

        deleteInfo.setEventId(deleteRequest.getAttributes().getId());
        deleteInfo.setCorrelationId(SapIoTAbstractionExtension.getExtension(deleteRequest).getCorrelationId());

        return deleteInfo;
    }

    private void handleSoftDelete(DeleteInfo deleteInfo) {
        try {
            String operationId = adxDataManager.deleteTimeSeries(deleteInfo);
            sendToOperationStorageQueue(deleteInfo, operationId, true);
        } catch (ADXClientException e) {
            statusQueueHelper.sendFailedDeleteToStatusQueue(deleteInfo, true);
            throw new DeleteTimeSeriesException("Unable to execute soft delete queue. Sending failed status to status queue.", e,
                    DeleteTimeSeriesErrorType.ADX_DATA_QUERY_EXCEPTION, IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY,
                    deleteInfo.getStructureId(), CommonConstants.CORRELATION_ID, deleteInfo.getCorrelationId()),
                    false);
        }
    }

    private void handlePurge(DeleteInfo deleteInfo) {
        try {
            String operationId = adxDataManager.purgeTimeSeries(deleteInfo.getStructureId(), Collections.singletonList(deleteInfo));
            sendToOperationStorageQueue(deleteInfo, operationId, false);
        } catch (ADXClientException e) {
            statusQueueHelper.sendFailedDeleteToStatusQueue(deleteInfo, false);
            throw new DeleteTimeSeriesException("Unable to execute purge queue. Sending failed status to status queue.", e,
                    DeleteTimeSeriesErrorType.ADX_DATA_QUERY_EXCEPTION, IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY,
                    deleteInfo.getStructureId(), CommonConstants.CORRELATION_ID, deleteInfo.getCorrelationId()), false);
        }
    }

    private void sendToOperationStorageQueue(DeleteInfo deleteInfo, String operationId, boolean softDelete) {
        OperationType operationType = softDelete ? OperationType.SOFT_DELETE : OperationType.PURGE;
        String operationName = softDelete ? "delete" : "purge";
        CloudQueueMessage storageQueueMessage = operationStorageQueueProcessor.getOperationInfoMessage(operationId,
                deleteInfo.getEventId(), deleteInfo.getStructureId(), deleteInfo.getCorrelationId(), operationType);

        try {
            operationStorageQueueProcessor.apply(new StorageQueueMessageInfo(storageQueueMessage, Optional.empty()));
        } catch (DeleteTimeSeriesException e) {
            DeleteTimeSeriesException outerException = new DeleteTimeSeriesException(String.format("Unable to add operation info to storage queue after " +
                    "successful %s operation.", operationName), e, DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR, e.getIdentifiers(), false);
            outerException.addIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY, deleteInfo.getStructureId());

            throw outerException;
        }
    }
}
