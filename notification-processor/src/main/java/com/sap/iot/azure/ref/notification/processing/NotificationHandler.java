package com.sap.iot.azure.ref.notification.processing;

import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties;
import com.sap.iot.azure.ref.integration.commons.model.mapper.CustomObjectMapper;
import com.sap.iot.azure.ref.notification.exception.NotificationErrorType;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.processing.model.EntityType;
import com.sap.iot.azure.ref.notification.util.Constants;
import lombok.AccessLevel;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NotificationHandler {

    private static final CustomObjectMapper mapper = new CustomObjectMapper();

    @Setter(AccessLevel.PACKAGE)
    private NotificationProcessorFactory notificationProcessorFactory = new NotificationProcessorFactory();

    /**
     * Depending on the notification entity type received in the notification message, the function fetches the corresponding Notification Processor.
     * The messages are internally mapped to {@link NotificationMessage} format along with partitionKey, and passed to executeProcessorHandler()
     * which invokes a specific operation based on the operation type.
     *
     * @param messages         incoming messages received from the notification processor function
     * @param systemProperties message application properties like offset details, partitionKey etc.
     */
    public void executeNotificationMessage(List<String> messages, Map<String, Object>[] systemProperties) throws RuntimeException {
        List<NotificationMessage> notificationMessages = getNotificationMessages(messages, systemProperties);
        for (NotificationMessage notificationMessage : notificationMessages) {
            EntityType entityType = notificationMessage.getType();
            try {
                if (notificationMessage.getType() == EntityType.SOURCEID || notificationMessage.getType() == EntityType.TAGS) {
                    InvocationContext.getLogger().log(Level.WARNING, "Source ID Type not yet supported");
                } else {
                    NotificationProcessor notificationProcessor = notificationProcessorFactory.getProcessor(Objects.requireNonNull(entityType));
                    executeProcessorHandler(notificationMessage, notificationProcessor);
                }
            } catch (NotificationProcessException e) {
                InvocationContext.getLogger().log(Level.SEVERE, "Notification processor exception for notification message:" + notificationMessage, e);
            }
        }
    }

    private List<NotificationMessage> getNotificationMessages(List<String> messages, Map<String, Object>[] systemProperties) {
        return IntStream.range(0, messages.size())
                .mapToObj(index -> {
                    Map<String, Object> systemPropertiesMap = SystemProperties.selectRelevantKeys(systemProperties[index]);
                    return ((Processor<String, NotificationMessage>) message -> {
                        return mapper.readValue(message, systemPropertiesMap, NotificationMessage.class,
                                SystemProperties.class);
                    }).apply(messages.get(index));
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private void executeProcessorHandler(NotificationMessage notificationMessage, NotificationProcessor notificationProcessor) {
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
