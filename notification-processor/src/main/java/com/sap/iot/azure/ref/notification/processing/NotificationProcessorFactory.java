package com.sap.iot.azure.ref.notification.processing;

import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.notification.exception.NotificationErrorType;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.processing.assignment.AssignmentNotificationProcessor;
import com.sap.iot.azure.ref.notification.processing.mapping.MappingNotificationProcessor;
import com.sap.iot.azure.ref.notification.processing.model.EntityType;
import com.sap.iot.azure.ref.notification.processing.structure.StructureNotificationProcessor;
import com.sap.iot.azure.ref.notification.util.Constants;

public class NotificationProcessorFactory {

    /**
     * Depending on the notificationProcessor type received, the function handles the creation
     * of required notification based on the processor type.
     *
     * @param type the {@link EntityType} from incoming notification message
     * @return the corresponding notification processor
     */
    public NotificationProcessor getProcessor(EntityType type) throws NotificationProcessException {

        switch (type) {
            case ASSIGNMENT:
                return new AssignmentNotificationProcessor();
            case MAPPING:
                return new MappingNotificationProcessor();
            case STRUCTURE:
                return new StructureNotificationProcessor();
            default:
                throw new NotificationProcessException("Notification processor not found for given type",
                        NotificationErrorType.UNKNOWN_TYPE_ERROR,
                        IdentifierUtil.getIdentifier(Constants.NOTIFICATION_PROCESSOR_TYPE, type.getName()),
                        false);
        }
    }
}
