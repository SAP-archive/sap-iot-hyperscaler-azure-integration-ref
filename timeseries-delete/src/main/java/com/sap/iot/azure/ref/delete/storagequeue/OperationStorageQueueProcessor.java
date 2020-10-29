package com.sap.iot.azure.ref.delete.storagequeue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.microsoft.azure.storage.queue.QueueRequestOptions;
import com.sap.iot.azure.ref.delete.connection.CloudQueueClientFactory;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.model.OperationInfo;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;

import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Optional;
import java.util.Random;

public class OperationStorageQueueProcessor {
    private final ObjectMapper mapper;
    private final CloudQueueClient cloudQueueClient;
    private final Random random = new SecureRandom();

    public OperationStorageQueueProcessor() {
        this(new CloudQueueClientFactory().getCloudQueueClient(), new ObjectMapper());
    }

    OperationStorageQueueProcessor(CloudQueueClient cloudQueueClient, ObjectMapper mapper) {
        this.cloudQueueClient = cloudQueueClient;
        this.mapper = mapper;
    }

    public void process(CloudQueueMessage message, Optional<Date> nextVisibleTimeOpt) throws DeleteTimeSeriesException {
        String messageIdKey = "messageId";

        try {
            // Retrieve a reference to a queue.
            CloudQueue queue = cloudQueueClient.getQueueReference(Constants.STORAGE_QUEUE_NAME);
            // Create the queue if it doesn't already exist.
            queue.createIfNotExists();
            Calendar calendar = GregorianCalendar.getInstance();
            int initialVisibilityDelayInSeconds = Constants.INITIAL_OPERATION_QUEUE_DELAY;

            if (nextVisibleTimeOpt.isPresent()) {
                //add timespan and write back into queue to continue monitoring
                calendar.setTime(nextVisibleTimeOpt.get());
                //get next visible time for message in queue. If it's < (configurable)10min -> increase visibility delay -> write in queue
                if (calendar.get(Calendar.MINUTE) < Constants.TARGET_TIME_MINUTES) {
                    int expBackoff = (int) Math.pow(2, calendar.getTimeInMillis());
                    int maxJitter = (int) Math.ceil(expBackoff * 0.2);
                    int finalBackoff = expBackoff + random.nextInt(maxJitter);
                    initialVisibilityDelayInSeconds = finalBackoff / 1000;
                }
            }
            RetryExponentialRetry retryExponentialRetry = new RetryExponentialRetry(Constants.STORAGE_QUEUE_MIN_BACKOFF,
                    Constants.STORAGE_QUEUE_DELTA_BACKOFF, Constants.STORAGE_QUEUE_MAX_BACKOFF, Constants.STORAGE_QUEUE_MAX_RETRIES);
            QueueRequestOptions queueRequestOptions = new QueueRequestOptions();
            queueRequestOptions.setRetryPolicyFactory(retryExponentialRetry);
            queue.addMessage(message, 0, initialVisibilityDelayInSeconds, queueRequestOptions, null);
        } catch (StorageException e) {
            throw new DeleteTimeSeriesException(String.format("Error while adding cloudQueueMessage to queue. HTTPStatusCode: %s and Error Code: %s",
                    e.getHttpStatusCode(), e.getErrorCode()), e, DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR,
                    IdentifierUtil.getIdentifier(messageIdKey, message.getId()), true);
        } catch (URISyntaxException e) {
            throw new DeleteTimeSeriesException("Invalid Storage Queue Address", e, DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR,
                    IdentifierUtil.getIdentifier(messageIdKey, message.getId()), false);
        }
    }

    public CloudQueueMessage getOperationInfoMessage(String operationId, String eventId, String structureId, String correlationId) throws DeleteTimeSeriesException {
        try {
            return new CloudQueueMessage(
                    mapper.writeValueAsString(
                            OperationInfo.builder()
                                    .operationId(operationId)
                                    .eventId(eventId)
                                    .structureId(structureId)
                                    .correlationId(correlationId)
                                    .build()));
        } catch (JsonProcessingException e) {
            throw new DeleteTimeSeriesException("Unable to form Operation Info object", e, DeleteTimeSeriesErrorType.JSON_PROCESSING_ERROR,
                    IdentifierUtil.getIdentifier("operationId", operationId, "eventId", eventId), false);
        }
    }
}
