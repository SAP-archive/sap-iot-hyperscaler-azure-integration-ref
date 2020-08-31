package com.sap.iot.azure.ref.integration.commons.exception;

import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;

/**
 * common error types that are used across multiple modules
 */
public enum CommonErrorType implements ErrorType {

    MAPPING_LOOKUP_ERROR("Mapping Lookup Error"),
    ADX_ERROR("Azure Data Explorer Error"),
    CACHE_ACCESS_ERROR("Redis Cache Access Error"),
    AVRO_EXCEPTION("Avro Parse Exception"),
    EVENT_HUB_ERROR("Event Hub Access Error"),
    JSON_PROCESSING_ERROR("Json Prcessing Error");

    private final String description;

    CommonErrorType(String description) {
        this.description = description;
    }

    @Override
    public String description() {
        return description;
    }
}
