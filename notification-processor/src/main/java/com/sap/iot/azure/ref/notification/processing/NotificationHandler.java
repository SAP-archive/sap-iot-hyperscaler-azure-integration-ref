package com.sap.iot.azure.ref.notification.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.notification.exception.NotificationErrorType;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.processing.model.EntityType;
import com.sap.iot.azure.ref.notification.util.Constants;
import lombok.AccessLevel;
import lombok.Setter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;

public class NotificationHandler {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Setter(AccessLevel.PACKAGE)
    private NotificationProcessorFactory notificationProcessorFactory = new NotificationProcessorFactory();

    /**
     * Depending on the notification entity type received in the notification message, the function fetches the corresponding Notification Processor.
     * The messages are internally mapped to {@link NotificationMessage} format along with partitionKey, and passed to executeProcessorHandler()
     * which invokes a specific operation based on the operation type.
     *
     * @param messages incoming messages received from the notification processor function
     * @param systemProperties message application properties like offset details, partitionKey etc.
     */
    public void executeNotificationHandling(List<String> messages, Map<String, Object>[] systemProperties) throws RuntimeException, IOException {
        List<NotificationMessage> notificationMessages = getNotificationMessages(messages, systemProperties);
        for (NotificationMessage notificationMessage : notificationMessages) {
            EntityType entityType = notificationMessage.getType();
            try {
                NotificationProcessor notificationProcessor = notificationProcessorFactory.getProcessor(Objects.requireNonNull(entityType));
                executeProcessorHandler(notificationMessage, notificationProcessor);
            } catch (NotificationProcessException e){
                InvocationContext.getLogger().log(Level.SEVERE, "Notification processor exception for notification message:"+ notificationMessage, e);
            }
        }
    }

    private List<NotificationMessage> getNotificationMessages(List<String> messages, Map<String, Object>[] systemProperties) throws IOException {
        List<NotificationMessage> notificationMessages = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            NotificationMessage notificationMessage = mapper.readValue(messages.get(i), NotificationMessage.class);
            notificationMessage.setPartitionKey(systemProperties[i].get("PartitionKey").toString());
            notificationMessages.add(notificationMessage);
        }
        return notificationMessages;
    }

    private void executeProcessorHandler(NotificationMessage notificationMessage, NotificationProcessor notificationProcessor){
        switch (notificationMessage.getOperation()) {
            case POST:
                notificationProcessor.handleCreateWithRetry(notificationMessage);
                break;
            case PATCH:
                notificationProcessor.handleUpdateWithRetry(notificationMessage);
                break;
            case DELETE:
                notificationProcessor.handleDeleteWithRetry(notificationMessage);
                break;
            default:
                throw new NotificationProcessException("Invalid operation type found for notification message",
                        NotificationErrorType.UNKNOWN_OPERATION_TYPE,
                        IdentifierUtil.getIdentifier(Constants.NOTIFICATION_PROCESSOR_TYPE, notificationMessage.getType().getName(),
                                Constants.NOTIFICATION_CHANGE_ENTITY, notificationMessage.getChangeEntity()), false);
        }
    }
}
