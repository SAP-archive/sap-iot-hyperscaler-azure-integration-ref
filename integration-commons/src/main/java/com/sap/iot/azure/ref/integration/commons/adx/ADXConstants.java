package com.sap.iot.azure.ref.integration.commons.adx;

public class ADXConstants {

    private ADXConstants() {
    }

    //Client
    public static final String ADX_RESOURCE_URI_PROP = "adx-resource-uri";
    public static final String ADX_INGESTION_URI_PROP = "adx-ingestion-resource-uri";
    public static final String SERVICE_PRINCIPAL_APPLICATION_CLIENT_ID_PROP = "service-principal-application-client-id";
    public static final String SERVICE_PRINCIPAL_APPLICATION_KEY_PROP = "service-principal-application-key";
    public static final String SERVICE_PRINCIPAL_AUTHORITY_ID_PROP = "service-principal-authority-id";
    public static final String ADX_DATABASE_NAME_PROP = "adx-database-name";
    //Mapping
    public static final String TABLE_PROPERTY_KEY = "Table";
    public static final String FORMAT_PROPERTY_KEY = "Format";
    public static final String MAPPING_PROPERTY_KEY = "IngestionMappingReference";
    public static final String MULTIJSON_FORMAT = "MULTIJSON";
    //Query & Mapping
    public static final String TABLE_PREFIX = "SAP__";
    public static final String MEASUREMENTS_PROPERTY_KEY = "measurements";
    //Data Types
    public static final String ADX_DATATYPE_STRING = "string";
    public static final String ADX_DATATYPE_DATETIME = "datetime";
    public static final String ADX_DATATYPE_BOOLEAN = "bool";
    public static final String ADX_DATATYPE_DYNAMIC = "dynamic";
    //Ingestion
    public static final String INGESTION_TYPE_PROP = "ingestion-type";
    //Columns
    public static final String DOC_STRING = "DocString";
    public static final String STATUS = "Status";
    public static final String SUCCEEDED = "Succeeded";
}
