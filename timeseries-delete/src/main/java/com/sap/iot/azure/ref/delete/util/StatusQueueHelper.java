package com.sap.iot.azure.ref.delete.util;

import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.delete.model.DeleteStatus;
import com.sap.iot.azure.ref.delete.model.DeleteStatusMessage;
import com.sap.iot.azure.ref.delete.output.DeleteStatusEventHubProcessor;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;

public class StatusQueueHelper {
    private final DeleteStatusEventHubProcessor deleteStatusEventHubProcessor;

    public StatusQueueHelper() {
        this(new DeleteStatusEventHubProcessor());
    }

    @VisibleForTesting
    StatusQueueHelper(DeleteStatusEventHubProcessor deleteStatusEventHubProcessor) {
        this.deleteStatusEventHubProcessor = deleteStatusEventHubProcessor;
    }

    /**
     * Sends a failed status to the status queue.
     * The error message is built depending on the soft delete parameter.
     *
     * @param deleteInfo describes the failed delete request
     * @param softDelete used for forming error message
     * @throws IoTRuntimeException exception while interacting with storage queue
     */
    public void sendFailedDeleteToStatusQueue(DeleteInfo deleteInfo, boolean softDelete) throws IoTRuntimeException {
        DeleteStatusMessage deleteStatusMessage = new DeleteStatusMessage();
        String operationName = softDelete ? "Soft delete" : "Purge";

        deleteStatusMessage.setStatus(DeleteStatus.FAILED);
        deleteStatusMessage.setEventId(deleteInfo.getEventId());
        deleteStatusMessage.setStructureId(deleteInfo.getStructureId());
        deleteStatusMessage.setError(String.format("%s operation failed for event id: ", operationName) + deleteInfo.getEventId());
        deleteStatusMessage.setCorrelationId(deleteInfo.getCorrelationId());
        //write to EventHub
        deleteStatusEventHubProcessor.apply(deleteStatusMessage);
    }

    /**
     * Sends a generic failed status to the status queue based on the event id.
     *
     * @param eventId used for forming error message
     * @throws IoTRuntimeException exception while interacting with storage queue
     */
    public void sendFailedDeleteToStatusQueue(String eventId) throws IoTRuntimeException {
        DeleteStatusMessage deleteStatusMessage = new DeleteStatusMessage();

        deleteStatusMessage.setStatus(DeleteStatus.FAILED);
        deleteStatusMessage.setEventId(eventId);
        deleteStatusMessage.setError("Delete operation failed for event id: " + eventId);
        //write to EventHub
        deleteStatusEventHubProcessor.apply(deleteStatusMessage);
    }
}
