package com.sap.iot.azure.ref.delete.util;

public class Constants {

    private Constants() {
    }
    public static final String TRIGGER_NAME = "delete";
    public static final String TRIGGER_EVENT_HUB_NAME = "sap.iot.abstraction.timeseries.delete.request";
    public static final String TRIGGER_EVENT_HUB_CONSUMER_GROUP = "sap-iot-timeseries-delete-request-cg";
    public static final String TRIGGER_EVENT_HUB_CONNECTION_STRING_PROP = "delete-timeseries-eventhub-connection-string";
    public static final String TRIGGER_SYSTEM_PROPERTIES_NAME = "SystemProperties";
    public static final String STORAGE_CONNECTION_STRING_PROP = "operation-storage-connection-string";
    public static final String DELETE_STATUS_EVENT_HUB_CONNECTION_STRING_PROP = "delete-status-eventhub-connection-string";
    public static final String STORAGE_QUEUE_NAME = "delete-operation-monitoring-queue";
    public static final int INITIAL_OPERATION_QUEUE_DELAY = 5;
    public static final int MAX_RETRIES = 5;
    public static final int TARGET_TIME_MINUTES = 10;
    public static final int STORAGE_QUEUE_MIN_BACKOFF = 1;
    public static final int STORAGE_QUEUE_DELTA_BACKOFF = 2;
    public static final int STORAGE_QUEUE_MAX_BACKOFF = 8;
    public static final int STORAGE_QUEUE_MAX_RETRIES = 3;
}
