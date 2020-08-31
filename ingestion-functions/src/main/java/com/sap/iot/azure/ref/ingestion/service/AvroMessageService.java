package com.sap.iot.azure.ref.ingestion.service;

import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

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
     * @return map {@link Map<String, List>} containing a list of {@link ProcessedMessage ProcessedMessages} grouped by source ID
     */
    public Map<String, List<ProcessedMessage>> createProcessedMessage(List<byte[]> avroMessages, Map<String, Object>[] systemProperties) {

        String sourceId;
        Map<String, List<ProcessedMessage>> processedMessagesMap = new HashMap<>();

        for (int i = 0; i < avroMessages.size(); i++) {

            Pair<String, List<ProcessedMessage>> p = processedMessages.apply(Pair.of(avroMessages.get(i), systemProperties[i]));
            if (p != null) {
                sourceId = p.getKey();
                processedMessagesMap.putIfAbsent(sourceId, new LinkedList<>());
                processedMessagesMap.get(sourceId).addAll(p.getValue());
            }
        }
        return processedMessagesMap;
    }
}
