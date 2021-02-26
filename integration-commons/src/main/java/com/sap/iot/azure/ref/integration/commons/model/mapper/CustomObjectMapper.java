package com.sap.iot.azure.ref.integration.commons.model.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.model.MessageEntity;

import java.util.Map;

public class CustomObjectMapper {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * deserializes a json string to the given object type along with message metadata
     *
     * @param messageContent message to be deserialized
     * @param sourceContent  message metadata as map
     * @param messageType    type of the deserialized message
     * @param sourceType     type of the message metadata
     * @param <M>            generic parameter for message POJO that extends from MessageEntity
     * @param <S>            generic parameter for source (message metadata)
     * @return POJO of type {@param messageType} along with source metadata
     * @throws IoTRuntimeException exception in json processing returned as non-transient exception
     */
    public <M extends MessageEntity<S>, S> M readValue(String messageContent, Map<String, Object> sourceContent,
                                                       Class<M> messageType, Class<S> sourceType) throws IoTRuntimeException {
        try {
            M m = objectMapper.readValue(messageContent, messageType);
            m.withSource(objectMapper.convertValue(sourceContent, sourceType));
            return m;
        } catch (JsonProcessingException e) {
            throw new IoTRuntimeException("Error in parsing message", CommonErrorType.JSON_PROCESSING_ERROR, InvocationContext.getContext().getInvocationId(),
                    objectMapper.convertValue(sourceContent, JsonNode.class), false);
        }
    }
}
