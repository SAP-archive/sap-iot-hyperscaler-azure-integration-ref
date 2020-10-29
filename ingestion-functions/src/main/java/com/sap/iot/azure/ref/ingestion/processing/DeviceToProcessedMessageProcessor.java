package com.sap.iot.azure.ref.ingestion.processing;

import com.google.common.collect.Maps;
import com.sap.iot.azure.ref.ingestion.exception.IngestionRuntimeException;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingHelper;
import com.sap.iot.azure.ref.integration.commons.model.mapping.SensorMappingInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMapping;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasure;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasureKey;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.Tag;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class DeviceToProcessedMessageProcessor implements Processor<Map.Entry<DeviceMeasureKey, List<DeviceMeasure>>, Map.Entry<String, ProcessedMessageContainer>> {
    private final MappingHelper mappingHelper;

    public DeviceToProcessedMessageProcessor() {
        this(new MappingHelper());
    }

    // visible for testing
    DeviceToProcessedMessageProcessor(MappingHelper mappingHelper) {
        this.mappingHelper = mappingHelper;
    }

    /**
     * Converts incoming device measures into the processed message format.
     * The device measures are augmented with the mapping information which is fetched from the {@link MappingHelper}.
     * The device measures are provided in groups. For every group, the same mapping
     * information applies. This way, the number of mapping information lookups are kept to a minimum.
     *
     * @param rawMessageGroupingListEntry, device measures grouped by {@link DeviceMeasureKey}
     * @return list of {@link ProcessedMessage ProcessedMessages} grouped by source ID
     * @throws IngestionRuntimeException any exception from mapping to ProcessedMessage format
     */
    @Override
    public Map.Entry<String, ProcessedMessageContainer> process(Map.Entry<DeviceMeasureKey, List<DeviceMeasure>> rawMessageGroupingListEntry) throws IngestionRuntimeException {
        return processRawMessagesGroup(rawMessageGroupingListEntry.getKey(), rawMessageGroupingListEntry.getValue());
    }

    private Map.Entry<String, ProcessedMessageContainer> processRawMessagesGroup(DeviceMeasureKey deviceMeasureKey, List<DeviceMeasure> rawMessages) throws IoTRuntimeException {

        //takes a raw messages grouped by raw message grouping key (sensorId & virtualCapabilityId) and transfers it to processedMessages grouped by sourceId
        SensorMappingInfo mapping = mappingHelper.getSensorMapping(deviceMeasureKey.getSensorId(), deviceMeasureKey.getVirtualCapabilityId());


        // form the common parts for all processed messages for this sourceId
        ProcessedMessage.ProcessedMessageBuilder processedMessageBuilder = ProcessedMessage.builder()
                .sourceId(mapping.getSourceId())
                .tags(mapping.getTags().stream().collect(Collectors.toMap(Tag::getTagSemantic, Tag::getTagValue)));

        // complete building processed message adding the measure with event timestamp
        List<ProcessedMessage> processedMessages = rawMessages.stream().map(rawMessage -> {
            return processedMessageBuilder
                    .measures(Collections.singletonList(mapDeviceMessageToApplicationModel(rawMessage, mapping)))
                    .build();
        }).collect(Collectors.toList());

        // add the schema along with message - avoids lookup to Redis Cache for building Avro Message in ProcessedTimeSeriesEventHubProcessor
        ProcessedMessageContainer processedMessageContainer = ProcessedMessageContainer.builder()
                .avroSchema(mapping.getSchemaInfo())
                .processedMessages(processedMessages)
                .structureId(mapping.getStructureId())
                .build();

        InvocationContext.getLogger().log(Level.FINE, String.format("Successfully processed %s messages for Sensor ID: '%s' and Virtual Capability ID: '%s'.",
                rawMessages.size(), deviceMeasureKey.getSensorId(), deviceMeasureKey.getVirtualCapabilityId()));
        return Maps.immutableEntry(mapping.getSourceId() + Constants.SEPARATOR + mapping.getStructureId(), processedMessageContainer);
    }

    private Map<String, Object> mapDeviceMessageToApplicationModel(DeviceMeasure rawMessage, SensorMappingInfo mapping) {
        Map<String, Object> measureValues = new HashMap<>();

        measureValues.put(Constants.TIMESTAMP_PROPERTY_KEY, rawMessage.getTimestamp().toEpochMilli());

        for (PropertyMapping pm : mapping.getPropertyMappings()) {
            if (rawMessage.getProperties().containsKey(pm.getCapabilityPropertyId())) {
                measureValues.put(pm.getStructurePropertyId(), rawMessage.getProperties().get(pm.getCapabilityPropertyId()));
            }
        }

        return measureValues;
    }
}