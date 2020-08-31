package com.sap.iot.azure.ref.integration.commons.exception.base;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;

/**
 * all exception in this application extends from IoTRuntimeException, thus allowing each exception to be classified either as transient / permanent type
 * that can be used in deciding the retry logic
 *
 */
public class IoTRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objMapper = new ObjectMapper();

    private final ErrorType errorType;
    private final String invocationId;
    private final JsonNode identifier;
    private final boolean isTransient;

    /**
     * IoTRuntimeException without a given exception {@param cause}
     * {@param errorType} can only be an enum implementing the ErrorType interface
     */
    public <T extends Enum<T> & ErrorType> IoTRuntimeException(String message, T errorType, String invocationId, JsonNode identifier,
                                                                  boolean isTransient) {
        super(message);
        this.errorType = errorType;
        this.invocationId = invocationId;
        this.identifier = identifier;
        this.isTransient = isTransient;
    }

    /**
     * IoTRuntimeException wrapping the given exception {@param cause}
     * {@param errorType} can only be an enum implementing the ErrorType interface
     */
    public <T extends Enum<T> & ErrorType> IoTRuntimeException(String message, Throwable cause, T errorType, String invocationId, JsonNode identifier,
                                                               boolean isTransient) {
        super(message, cause);
        this.errorType = errorType;
        this.invocationId = invocationId;
        this.identifier = identifier;
        this.isTransient = isTransient;
    }

    /**
     * Returns a {@link Boolean} indicating if this exception is transient.
     *
     * @return {@link Boolean}
     */
    public boolean isTransient() {
        return isTransient;
    }

    /**
     * Wraps a {@link Exception} and context information without throwable cause as non-transient {@link IoTRuntimeException}.
     *
     * @param identifier, provides context information about the occurring exception
     * @param errorType,  categorization of the occurring error
     * @param message,    exception message
     * @return {@link IoTRuntimeException} which contains the wrapped exception and error information
     */
    public static <T extends Enum<T> & ErrorType> IoTRuntimeException wrapNonTransient(JsonNode identifier, T errorType, String message) {
        return new IoTRuntimeException(message, errorType, InvocationContext.getContext().getInvocationId(), identifier, false);
    }

    /**
     * Wraps a {@link Exception} and context information without throwable cause as transient {@link IoTRuntimeException}.
     *
     * @param identifier, provides context information about the occurring exception
     * @param errorType,  categorization of the occurring error
     * @param message,    exception message
     * @return {@link IoTRuntimeException} which contains the wrapped exception and error information
     */
    public static <T extends Enum<T> & ErrorType> IoTRuntimeException wrapTransient(JsonNode identifier, T errorType, String message) {
        return new IoTRuntimeException(message, errorType, InvocationContext.getContext().getInvocationId(), identifier, true);
    }

    /**
     * Wraps a {@link Exception} and context information with throwable cause {@param cause} as non-transient {@link IoTRuntimeException}.
     *
     * @param identifier, provides context information about the occurring exception
     * @param errorType,  categorization of the occurring error
     * @param cause,         {@link Exception} which will be wrapped
     * @param message,    exception message
     * @return {@link IoTRuntimeException} which contains the wrapped exception and error information
     */
    public static <T extends Enum<T> & ErrorType> IoTRuntimeException wrapNonTransient(JsonNode identifier, T errorType, String message, Throwable cause) {
        return new IoTRuntimeException(message, cause, errorType, InvocationContext.getContext().getInvocationId(), identifier, false);
    }

    /**
     * Wraps a {@link Exception} and context information without throwable cause {@param cause} as transient {@link IoTRuntimeException}.
     *
     * @param identifier, provides context information about the occurring exception
     * @param errorType,  categorization of the occurring error
     * @param message,    exception message
     * @param cause,         {@link Exception} which will be wrapped
     * @return {@link IoTRuntimeException} which contains the wrapped exception and error information
     */
    public static <T extends Enum<T> & ErrorType> IoTRuntimeException wrapTransient(JsonNode identifier, T errorType, String message, Throwable cause) {
        return new IoTRuntimeException(message, cause, errorType, InvocationContext.getContext().getInvocationId(), identifier, true);
    }

    /**
     * Adds entry to identifier json node.
     *
     * @param key,  key for the new identifier entry
     * @param value,  value for the new identifier entry
     */
    public void addIdentifier(String key, String value) {
        ((ObjectNode)identifier).put(key, value);
    }

    /**
     * Return an ObjectNode containing the context information of this exception.
     *
     * @return {@link ObjectNode} containing this exceptions context information
     */
    public ObjectNode jsonify() {

        ObjectNode objectNode = objMapper.createObjectNode();
        objectNode.set("Identifier", this.identifier);
        objectNode.put("ErrorCode", this.errorType.name());
        objectNode.put("Description", this.errorType.description());
        objectNode.put("Message", super.getMessage());
        objectNode.put("InvocationId", this.invocationId);
        objectNode.put("Transient", this.isTransient);

        return objectNode;
    }

    /**
     * returns the error type
     * @return error type
     */
    public ErrorType getErrorType() {
        return errorType;
    }

    /**
     * add the identifier to the default message
     * @return message enriched with identifier corresponding to data object impacted (e.g., sensorId, structureId, etc.,)
     */
    @Override
    public String getMessage() {
        return jsonify().toString();
    }

}
