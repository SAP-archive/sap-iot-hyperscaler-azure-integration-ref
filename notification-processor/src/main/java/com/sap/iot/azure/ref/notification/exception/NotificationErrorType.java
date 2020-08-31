package com.sap.iot.azure.ref.notification.exception;

import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;

/**
 * common error types that are used across multiple modules
 */
public enum NotificationErrorType implements ErrorType {

    DATA_TYPE_ERROR("Data Type Error"),
    UNKNOWN_TYPE_ERROR("Notification Type Not Found"),
    NOTIFICATION_PARSER_ERROR("Unable to parse entity data to the expected format"),
    UNKNOWN_OPERATION_TYPE("Unknown operation type found for notification message");
    private final String description;

    NotificationErrorType(String description) {
        this.description = description;
    }

    @Override
    public String description() {
        return description;
    }
}

