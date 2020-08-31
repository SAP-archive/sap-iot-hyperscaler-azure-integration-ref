package com.sap.iot.azure.ref.device.management.exception;

import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;

public enum DeviceManagementErrorType implements ErrorType {
    IOTHUB_ERROR("IoT Hub Error"),
    EVENT_HUB_ERROR("Event Hub Error"),
    INVALID_CLOUD_EVENT_TYPE("Invalid CloudEvents Type"),
    INVALID_CLOUD_EVENT_MESSAGE("Invalid CloudEvents Message"),
    DEVICE_MANAGEMENT_ERROR("Device Management Error - Generic");

    private final String description;

    DeviceManagementErrorType(String description) {
        this.description = description;
    }

    @Override
    public String description() {
        return description;
    }
}
