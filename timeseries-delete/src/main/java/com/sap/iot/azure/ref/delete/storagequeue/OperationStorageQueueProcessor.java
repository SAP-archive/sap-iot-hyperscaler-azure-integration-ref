package com.sap.iot.azure.ref.delete.storagequeue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.connection.CloudQueueClientFactory;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.model.OperationInfo;
import com.sap.iot.azure.ref.delete.model.OperationType;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;

import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Optional;
import java.util.Random;

public class OperationStorageQueueProcessor implements Processor<StorageQueueMessageInfo, Void> {
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

    /**
     * Sends a {@link StorageQueueMessageInfo} to the operation storage queue with the name defined in {@link Constants#STORAGE_QUEUE_NAME}.
     * An exponential backoff is used to increase the visibility of the message. The maximum time is defined in {@link Constants#TARGET_TIME_MINUTES}.
     * The built-in retry mechanic of the storage queue is used to recover from transient errors.
     *
     * @param messageInfo info to be forwarded to storage queue
     * @throws DeleteTimeSeriesException exception in storage queue interaction
     */
    public Void process(StorageQueueMessageInfo messageInfo) throws DeleteTimeSeriesException {
        CloudQueueMessage message = messageInfo.getCloudQueueMessage();
        Optional<Date> nextVisibleTimeOpt = messageInfo.getNextVisibleTimeOpt();

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
            queue.addMessage(message, 0, initialVisibilityDelayInSeconds, StorageQueueUtil.getRetryOptions(), null);
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

    /**
     * Creates a {@link CloudQueueMessage} from the provided operation information.
     * The message content is based on the {@link OperationInfo} POJO and is formatted as json string.
     *
     * @param operationId id of the delete operation
     * @param eventId id of the according cloud event
     * @param structureId id of the affected structure
     * @param correlationId id for logging and error tracing purposes
     * @param operationType type of delete operation
     * @return cloud queue message containing all relevant operation information
     * @throws DeleteTimeSeriesException exception while forming operation info object
     */
    public CloudQueueMessage getOperationInfoMessage(String operationId, String eventId, String structureId, String correlationId,
                                                     OperationType operationType) throws DeleteTimeSeriesException {
        try {
            return new CloudQueueMessage(
                    mapper.writeValueAsString(
                            OperationInfo.builder()
                                    .operationId(operationId)
                                    .eventId(eventId)
                                    .structureId(structureId)
                                    .correlationId(correlationId)
                                    .operationType(operationType)
                                    .build()));
        } catch (JsonProcessingException e) {
            throw new DeleteTimeSeriesException("Unable to form Operation Info object", e, DeleteTimeSeriesErrorType.JSON_PROCESSING_ERROR,
                    IdentifierUtil.getIdentifier("operationId", operationId, "eventId", eventId), false);
        }
    }
}
