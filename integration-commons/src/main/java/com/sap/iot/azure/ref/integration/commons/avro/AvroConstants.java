package com.sap.iot.azure.ref.integration.commons.avro;

public class AvroConstants {

    private AvroConstants() {
    }

    // AvroHelper
    public static final String AVRO_DATUM_KEY_MESSAGE_ID = "messageId";
    public static final String AVRO_DATUM_KEY_IDENTIFIER = "identifier";
    public static final String AVRO_DATUM_KEY_STRUCTURE_ID = "structureId";
    public static final String AVRO_DATUM_KEY_TENANT = "tenant";
    public static final String AVRO_DATUM_KEY_TAGS = "tags";
    public static final String AVRO_DATUM_KEY_MEASUREMENTS = "measurements";
    public static final String AVRO_SCHEMA_UNION = "union";
    public static final String AVRO_SCHEMA_NULL = "null";
    public static final String GDPR_DATA_CATEGORY = "gdprDataCategory";
    public static final String GDPR_CATEGORY_PII = "com.sap.appiot.security:pii";
    public static final String GDPR_CATEGORY_SPI = "com.sap.appiot.security:spi";
}
