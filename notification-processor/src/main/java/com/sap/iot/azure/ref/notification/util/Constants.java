package com.sap.iot.azure.ref.notification.util;

public class Constants {

    private Constants() {
    }
    //Notification Processor Function Constants
    public static final String TRIGGER_NAME = "notification";
    public static final String TRIGGER_EVENT_HUB_NAME = "sap.iot.modelabstraction.meta.change.v1";
    public static final String TRIGGER_EVENT_HUB_CONSUMER_GROUP = "sap-iot-notification-handler-cg";
    public static final String TRIGGER_EVENT_HUB_CONNECTION_STRING_PROP = "notification-eventhub-connection-string";
    public static final String TRIGGER_SYSTEM_PROPERTIES_NAME = "SystemPropertiesArray";
    public static final int MAX_RETRIES = 5;
    public static final String NOTIFICATION_PROCESSOR_TYPE = "type";
    public static final String NOTIFICATION_CHANGE_ENTITY = "changeEntity";
    //Structure Notifications
    public static final String DATA_TYPE_KEY = "dataType";
}