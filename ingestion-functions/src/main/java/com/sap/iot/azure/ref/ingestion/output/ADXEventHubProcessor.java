package com.sap.iot.azure.ref.ingestion.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.adx.ADXConstants;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.connection.EventHubClientFactory;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.eventhub.BaseEventHubProcessor;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

public class ADXEventHubProcessor extends BaseEventHubProcessor<ProcessedMessageContainer> implements Processor<Map.Entry<String, ProcessedMessageContainer>,
        CompletableFuture<Void>> {

    private static final String CONNECTION_STRING = System.getenv(Constants.ADX_SOURCE_CONNECTION_STRING_PROP);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public ADXEventHubProcessor() {
        this(new EventHubClientFactory().getEhClient(CONNECTION_STRING));
    }

    @VisibleForTesting
    ADXEventHubProcessor(CompletableFuture<EventHubClient> eventHubClientFuture) {
        super(eventHubClientFuture);
    }

    /**
     * Send a single group of processed messages to ADX source Event Hub with the key as partition key.
     * The {@link ProcessedMessage ProcessedMessages} are formatted into a structure which is compatible with the ADX ingestion.
     * Since the ADX ingestion requires the table information, the messages will include the table information in the event data properties.
     *
     * @param processedMessageGroup, single list of processed messages grouped by a key
     * @return completable future from sending adx message to event hub
     */
    @Override
    public CompletableFuture<Void> process(Map.Entry<String, ProcessedMessageContainer> processedMessageGroup) {
        return super.process(processedMessageGroup.getValue(), processedMessageGroup.getKey());
    }

    @Override
    protected List<EventData> createEventData(ProcessedMessageContainer processedMessageContainer) {
        List<EventData> eventDataList = new ArrayList<>();

        for (ProcessedMessage processedMessage : processedMessageContainer.getProcessedMessages()) {
            String sourceId = processedMessage.getSourceId();
            String structureId = processedMessage.getStructureId();
            Map<String, String> tags = processedMessage.getTags();

            // ProcessedMessage measures always has one measure
            processedMessage.getMeasures().forEach(measure -> {
                try {
                    eventDataList.add(convertToEventData(measure, sourceId, structureId, tags));
                } catch (JsonProcessingException e) {
                    InvocationContext.getLogger().log(Level.SEVERE, "Unable to create Event Data from message", e);
                }
            });
        }

        return eventDataList;
    }

    private EventData convertToEventData(Map<String, Object> measure, String sourceId, String structureId, Map<String, String> tags) throws JsonProcessingException {
        ObjectNode adxMessage = objectMapper.createObjectNode();

        String timestamp = Instant.ofEpochMilli(Long.parseLong(measure.get(CommonConstants.TIMESTAMP_PROPERTY_KEY).toString())).toString();
        ObjectNode adxMeasurements = objectMapper.valueToTree(measure);
        adxMeasurements.remove(CommonConstants.TIMESTAMP_PROPERTY_KEY);

        // add source id and _time to the message
        adxMessage.put(CommonConstants.SOURCE_ID_PROPERTY_KEY, sourceId);
        adxMessage.put(CommonConstants.TIMESTAMP_PROPERTY_KEY, timestamp);

        //Add Tags
        tags.forEach(adxMeasurements::put);

        //Add Measurements to measure
        adxMessage.set(ADXConstants.MEASUREMENTS_PROPERTY_KEY, adxMeasurements);

        EventData eventData = EventData.create(objectMapper.writeValueAsBytes(adxMessage));

        //Add ADX Mapping Info
        String tableName = ADXConstants.TABLE_PREFIX + structureId;
        eventData.getProperties().put(ADXConstants.TABLE_PROPERTY_KEY, tableName);
        eventData.getProperties().put(ADXConstants.FORMAT_PROPERTY_KEY, ADXConstants.MULTIJSON_FORMAT);
        eventData.getProperties().put(ADXConstants.MAPPING_PROPERTY_KEY, tableName);

        return eventData;
    }
}
