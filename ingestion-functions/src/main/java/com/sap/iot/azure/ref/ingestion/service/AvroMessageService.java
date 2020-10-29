package com.sap.iot.azure.ref.ingestion.service;

import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroMessageService {

    private final ProcessMessageService processedMessages;

    public AvroMessageService() {
        this(new ProcessMessageService());
    }

    @VisibleForTesting
    AvroMessageService(ProcessMessageService processedMessages) {
        this.processedMessages = processedMessages;
    }

    /**
     * Creates a map of processed messages, given a list of avroMessages and systemProperties.
     * The apply method of processMessages is invoked for each pair of avroMessage and systemProperty for further deserialization of messages.
     *
     * @param avroMessages, required for converting byte to string for deserialization
     * @param systemProperties, required for fetching sourceId and structureId
     * @return map {@link Map<String, ProcessedMessageContainer>} containing a list of {@link ProcessedMessage ProcessedMessages} grouped by source ID
     */
    public Map<String, ProcessedMessageContainer> createProcessedMessage(List<byte[]> avroMessages, Map<String, Object>[] systemProperties) {

        String sourceId;
        Map<String, ProcessedMessageContainer> processedMessagesMap = new HashMap<>();

        for (int i = 0; i < avroMessages.size(); i++) {

            Pair<String, ProcessedMessageContainer> p = processedMessages.apply(Pair.of(avroMessages.get(i), systemProperties[i]));
            if (p != null) {
                sourceId = p.getKey();

                // add processed messages to the existing ProcessedMessageContainer already added
                processedMessagesMap.computeIfPresent(sourceId, (sourceId2, container) -> container.addAll(p.getValue()));

                // create a new processed message container
                processedMessagesMap.putIfAbsent(sourceId, p.getValue());
            }
        }
        return processedMessagesMap;
    }
}
