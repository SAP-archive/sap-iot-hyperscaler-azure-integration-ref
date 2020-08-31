package com.sap.iot.azure.ref.integration.commons.mapping;

public class MappingServiceConstants {

    private MappingServiceConstants() {

    }

    //Mapping App
    public static final String MAPPING_APP_HOST_PROP = "mapping-app-host";
    public static final String MAPPING_APP_HOST = System.getenv(MAPPING_APP_HOST_PROP);
    public static final String SENSOR_ID_PLACEHOLDER = "{SENSOR_ID}";
    public static final String ASSIGNMENT_ENDPOINT = MAPPING_APP_HOST + "/Model/v1/Assignments?$expand=Sensors&$filter=Sensors/SensorId eq '" + SENSOR_ID_PLACEHOLDER + "'&$format=json";
    public static final String MAPPING_ID_PLACEHOLDER = "{MAPPING_ID}";
    public static final String MAPPING_ENDPOINT = MAPPING_APP_HOST + "/Model/v1/Mappings('" + MAPPING_ID_PLACEHOLDER + "')?$expand=Measures, Measures/PropertyMeasures&$format=json";

    //Lookup App
    public static final String LOOKUP_APP_HOST_PROP = "lookup-app-host";
    public static final String LOOKUP_APP_HOST = System.getenv(LOOKUP_APP_HOST_PROP);
    public static final String TENANT_PROP = "sap-iot-tenant";
    public static final String TENANT = System.getenv(TENANT_PROP);

    //Tags
    public static final String VIRTUAL_CAPABILITY_ID_PLACEHOLDER = "{V_CAP_ID}";
    public static final String TAGS_ENDPOINT = LOOKUP_APP_HOST + "/v1/Lookup/Tags?SensorId=" + SENSOR_ID_PLACEHOLDER + "&CapabilityId=" + VIRTUAL_CAPABILITY_ID_PLACEHOLDER;

    //Schema
    public static final String STRUCTURE_ID_PLACEHOLDER = "{STRUCTURE_ID}";
    public static final String SCHEMA_ENDPOINT = LOOKUP_APP_HOST + "/v1/Lookup/AvroSchema?StructureId=" + STRUCTURE_ID_PLACEHOLDER;

    //Cache
    public static final String CACHE_KEY_CREATOR_PREFIX = "SAP_";
    public static final String CACHE_SENSOR_KEY_PREFIX = "SENSOR_";
    public static final String CACHE_MAPPING_KEY_PREFIX = "MAPPING_";
    public static final String CACHE_STRUCTURE_KEY_PREFIX = "STRUCTURE_";
    public static final String CACHE_KEY_SEPARATOR = "_";

    //Token
    public static final String TOKEN_ENDPOINT_PROP = "token-endpoint";
    public static final String TOKEN_ENDPOINT = System.getenv(TOKEN_ENDPOINT_PROP) + "/oauth/token";
    public static final String BEARER_TOKEN_PREFIX = "Bearer ";
    public static final String X_REPLAY_HEADER = "X-Replay";
    public static final String CLIENT_ID_PROP = "client-id";
    public static final String CLIENT_ID = System.getenv(CLIENT_ID_PROP);
    public static final String CLIENT_SECRET_PROP = "client-secret";
    public static final String CLIENT_SECRET = System.getenv(CLIENT_SECRET_PROP);
    public static final String TOKEN_BODY_KEY = "access_token";
    public static final String TOKEN_URL_PROPERTY_KEY = "tokenUrl";
    public static final String GRANT_TYPE = "grant_type";
    public static final String CLIENT_ID_KEY = "client_id";
    public static final String CLIENT_SECRET_KEY = "client_secret";
    public static final String RESPONSE_TYPE = "response_type";
    public static final String REQUESTED_SCOPES = "scope";
    public static final String CLIENT_CREDENTIALS = "client_credentials";
    public static final String RESPONSE_TYPE_TOKEN = "token";

    //API Response Body Keys
    public static final String API_RESPONSE_BODY_KEY_VALUE = "value";
    public static final String API_RESPONSE_BODY_KEY_D = "d";
    public static final String API_RESPONSE_BODY_KEY_MEASURES = "Measures";
    public static final String API_RESPONSE_BODY_KEY_RESULTS = "results";
    public static final String SERVICE_NAME_KEY = "serviceName";

    //HTTP Response
    public static final String TRUE = "true";
}
