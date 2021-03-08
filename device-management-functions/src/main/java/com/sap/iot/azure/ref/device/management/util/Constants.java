package com.sap.iot.azure.ref.device.management.util;

public class Constants {

	public static final String DEVICE_MANAGEMENT_FUNCTION = "DeviceManagement";

	public static final String TRIGGER_NAME = "deviceManagementRequest";
	public static final String TRIGGER_EVENT_HUB_NAME = "sap.iot.abstraction.device.management.request";
	public static final String TRIGGER_EVENT_HUB_CONNECTION_STRING_PROP = "device-management-request-connection-string";
	public static final String TRIGGER_EVENT_HUB_CONSUMER_GROUP = "sap-iot-device-management-request-cg";

	public static final String IOTHUB_NAME = "iothub-name";
	public static final String IOTHUB_REGISTRY_CONNECTION_STRING_PROP = "iothub-registry-connection-string";
	public static final String DEVICE_MANAGEMENT_STATUS_CONNECTION_STRING_PROP = "device-management-status-connection-string";
	public static final int MAX_RETRIES = 5;
}
