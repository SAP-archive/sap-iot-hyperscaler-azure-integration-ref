package com.sap.iot.azure.ref.integration.commons.model.timeseries.processed;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * container for a group of processed message with the same structure
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedMessageContainer {
    private String avroSchema;
    private String structureId;
    private List<ProcessedMessage> processedMessages;

    public Optional<String> getAvroSchema() {
        return Optional.ofNullable(avroSchema);
    }

    public ProcessedMessageContainer(String structureId, List<ProcessedMessage> processedMessages) {
        this.structureId = structureId;
        this.processedMessages = processedMessages;
    }

    public ProcessedMessageContainer addAll(ProcessedMessageContainer other) {
        processedMessages.addAll(other.getProcessedMessages());
        return this;
    }
}
