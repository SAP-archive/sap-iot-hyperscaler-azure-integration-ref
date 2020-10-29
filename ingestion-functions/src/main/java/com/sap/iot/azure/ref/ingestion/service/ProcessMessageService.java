package com.sap.iot.azure.ref.ingestion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.ingestion.exception.IngestionRuntimeException;
import com.sap.iot.azure.ref.ingestion.util.AvroMessageConverter;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.avro.AvroConstants;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ProcessMessageService implements Processor<Pair<byte[], Map<String, Object>>, Pair<String, ProcessedMessageContainer>> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final AvroMessageConverter avroMessageConverter;

    ProcessMessageService() {
        this(new AvroMessageConverter());
    }

    @VisibleForTesting
    ProcessMessageService(AvroMessageConverter avroMessageConverter) {
        this.avroMessageConverter = avroMessageConverter;
    }

    /**
     * Creates a pair of list of processed messages grouped by the sourceId, given a pair of avroMessage and systemProperty.
     * The createProcessedMessage method returns the required response by deserializing the message for a
     * particular structureId, extracted from {@link Constants#SYSTEM_PROPERTIES_PARTITION_KEY}.
     * Using the response returned after deserialization of avroMessage, {@link ProcessedMessage ProcessedMessages}
     * map is built using information like sourceId, structureId, tags, measuresList, & tenantId.
     *
     * @param t, a pair of avroMessage and systemProperty.
     * @return pair {@link Pair<String, List>} containing sourceId and list of {@link ProcessedMessage ProcessedMessages}.
     */
    @Override
    public Pair<String, ProcessedMessageContainer> process(Pair<byte[], Map<String, Object>> t) throws IngestionRuntimeException {
        return createProcessedMessage(t.getKey(), t.getValue());
    }

    @SuppressWarnings("unchecked")
    private Pair<String, ProcessedMessageContainer> createProcessedMessage(byte[] message,
                                                                        Map<String, Object> systemProperties) {

        String partitionKey;
        String sourceId;
        String structureId;
        List<JsonNode> genericJSONMessages;
        List<ProcessedMessage> processedMessages = new LinkedList<>();

        partitionKey = ((String) systemProperties.get(Constants.SYSTEM_PROPERTIES_PARTITION_KEY));
        int d = partitionKey.lastIndexOf(Constants.SEPARATOR);
        if (d == -1) {
            throw IoTRuntimeException.wrapNonTransient(IdentifierUtil.getIdentifier("PartitionKey", partitionKey), CommonErrorType.AVRO_EXCEPTION,
                    "sourceId and structureId cannot be identified from Partition Key");
        } else {
            sourceId = partitionKey.substring(0, d);
            structureId = partitionKey.substring(d + 1);
        }

        genericJSONMessages = avroMessageConverter.deserializeAvroMessage(structureId, message);

        ProcessedMessage pm;
        for (JsonNode jsonMessage : genericJSONMessages) {

            List<Map<String, Object>> measuresList = new LinkedList<>();
            for (JsonNode measures : jsonMessage.get(AvroConstants.AVRO_DATUM_KEY_MEASUREMENTS)) {
                measuresList.add(objectMapper.convertValue(measures, Map.class));
            }

            // Note: even though the tags is represented as array (allowing for extensibility), in current implementation the tags array has only one entry
            if (jsonMessage.get(AvroConstants.AVRO_DATUM_KEY_TAGS).size() > 1) {
                throw IoTRuntimeException.wrapNonTransient(IdentifierUtil.getIdentifier("sourceId", sourceId, "structureId", structureId),
                        CommonErrorType.AVRO_EXCEPTION, "Multiple tags are provided for the same Source Id");
            }

            Map<String, String> tagMap = null;
            for (JsonNode tag : jsonMessage.get(AvroConstants.AVRO_DATUM_KEY_TAGS)) {
                tagMap = objectMapper.convertValue(tag, Map.class);
            }
            if (tagMap != null && !tagMap.isEmpty()) {
                while (tagMap.values().remove(null));
            }

            pm = ProcessedMessage.builder()
                    .sourceId(sourceId)
                    .measures(measuresList)
                    .tags(tagMap)
                    .build();

            processedMessages.add(pm);

        }

        return Pair.of(sourceId, new ProcessedMessageContainer(structureId, processedMessages));
    }
}
