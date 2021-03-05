package com.sap.iot.azure.ref.ingestion.util;

public class Constants {

    private Constants() {
    }

    //Mapping Function Constants
    public static final String INGESTION_FUNCTION = "Ingestion";
    public static final String TRIGGER_NAME = "message";
    public static final String TRIGGER_EVENT_HUB_NAME = "sap-iot-hs-iot-hub";
    public static final String TRIGGER_IOT_HUB_CONSUMER_GROUP = "sap-iot-ingestion-mapping-cg";
    public static final String TRIGGER_IOT_HUB_CONNECTION_STRING_PROP = "iothub-connection-string";
    public static final String SEPARATOR = "/";
    public static final String IOT_HUB_ENQUEUED_TIME = "iothub-enqueuedtime";
    public static final String IOT_HUB_DEVICE_ID = "iothub-connection-device-id";


    //AvroParser Function Constants
    public static final String AVRO_PARSER_FUNCTION = "AvroParser";
    public static final String PROCESSED_TIME_SERIES_IN_CONNECTION_STRING = "processed-timeseries-in-connection-string";
    public static final String PROCESSED_TIME_SERIES_IN_EVENT_HUB = "sap.iot.abstract.processed_timeseries_in.v1";
    public static final String PROCESSED_TIME_SERIES_IN_EVENT_HUB_CONSUMER_GROUP = "sap-iot-ingestion-avro-parser-cg";

    //EventHub Constants
    public static final String PROCESSED_TIME_SERIES_CONNECTION_STRING_PROP = "processed-timeseries-connection-string";
    public static final String ADX_SOURCE_CONNECTION_STRING_PROP = "adx-source-connection-string";

    //Device Payload Mapper Constants
    public static final String TRANSFORM_DEFAULT_TYPE_PROP = "transform-default-message-type";
    public static final String TRANSFORM_DEFAULT_TYPE = System.getenv(TRANSFORM_DEFAULT_TYPE_PROP);
    public static final String TRANSFORM_TYPE_IOT_DEVICE_MODEL = "SAPIoTDeviceModel";
    public static final int MAX_RETRIES = 5;

    //System Properties Constants

    public static final String SYSTEM_PROPERTIES_PARTITION_KEY = "PartitionKey";

    //Time Series Constants
    public static final String TIMESTAMP_PROPERTY_KEY = "_time";
}