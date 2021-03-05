package com.sap.iot.azure.ref.integration.commons.constants;

import com.sap.iot.azure.ref.integration.commons.util.EnvUtils;

public class CommonConstants {

    private CommonConstants() {
    }

    // Cache Repository
    public static final String CACHE_KEY = "cacheKey";

    // message properties
    public static final String SYSTEM_PROPERTIES = "System Properties";
    public static final String PARTITION_ID = "PartitionId";
    public static final String BATCH_OFFSET_START = "OffsetStart";
    public static final String BATCH_OFFSET_END = "OffsetEnd";
    public static final String BATCH_ENQUEUED_TIME_START = "EnqueuedTimeStart";
    public static final String BATCH_ENQUEUED_TIME_END = "EnqueuedTimeEnd";
    public static final String OFFSET = "Offset";
    public static final String ENQUEUED_TIME_UTC = "EnqueuedTimeUtc";
    public static final String SEQUENCE_NUMBER = "SequenceNumber";

    // Time Series Constants
    public static final String SOURCE_ID_PROPERTY_KEY = "sourceId";
    public static final String STRUCTURE_ID_PROPERTY_KEY = "structureId";
    public static final String SENSOR_ID_PROPERTY_KEY = "sensorId";
    public static final String CAPABILITY_ID_PROPERTY_KEY = "CapabilityId";
    public static final String VIRTUAL_CAPABILITY_ID_PROPERTY_KEY = "virtualCapabilityId";
    public static final String TIMESTAMP_PROPERTY_KEY = "_time";
    public static final String ENQUEUED_TIME_PROPERTY_KEY = "_enqueued_time";
    public static final String ENQUEUED_TIME_PROPERTY_VALUE = "x-opt-enqueued-time";
    public static final String DELETED_PROPERTY_KEY = "_isDeleted";
    public static final String DELETED_PROPERTY_VALUE = "false";
    public static final String MAPPING_ID_PROPERTY_KEY = "mappingId";
    public static final String ASSIGNMENT_ID = "assignmentId";
    public static final String PARTITION_KEY = "PartitionKey";
    public static final String OPERATION_ID = "operationId";
    public static final String CORRELATION_ID = "correlationId";
    public static final String MESSAGE_ID = "messageId";
    public static final String IOT_HUB_DEVICE_ID = "iothub-connection-device-id";

    // common function trigger constants
    public static final String TRIGGER_SYSTEM_PROPERTIES_ARRAY_NAME = "SystemPropertiesArray";
    public static final String TRIGGER_EVENT_HUB_DATA_TYPE_BINARY = "binary";
    public static final String PARTITION_CONTEXT = "PartitionContext";

    // EventHub SKU Tier Information
    public static final String EVENTHUB_SKU_STANDARD_TIER = "Standard";
    public static final String EVENTHUB_SKU_BASIC_TIER = "Basic";
    public static final String EVENTHUB_SKU_NAME = "eventhub-sku-tier";
    public static final String EVENTHUB_SKU_TIER = EnvUtils.getEnv(EVENTHUB_SKU_NAME, EVENTHUB_SKU_BASIC_TIER);
    public static final int EVENTHUB_SKU_STANDARD_TIER_SIZE = 1024;
    public static final int EVENTHUB_SKU_BASIC_TIER_SIZE = 256;
    
}