package com.sap.iot.azure.ref.integration.commons.exception.base;

/**
 * All error types ENUMs extend from this interface. This allows each module to define their own set of error type enums
 * It's not expected to have a class implementing this ErrorType. In all places where the error type is accessed, this interface shall be used
 */
public interface ErrorType {

    /**
     * error code for this error type
     * @return error code (fixed values from the extending ENUMS)
     */
    String name();

    /**
     * readable name / description for the error code
     * @return readable name of the error code
     */
    String description();

}
