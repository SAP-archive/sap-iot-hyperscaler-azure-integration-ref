package com.sap.iot.azure.ref.delete.output;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.model.DeleteStatusData;
import com.sap.iot.azure.ref.delete.model.DeleteStatusMessage;
import com.sap.iot.azure.ref.delete.model.cloudEvents.SapIoTAbstractionExtension;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.connection.EventHubClientFactory;
import com.sap.iot.azure.ref.integration.commons.eventhub.BaseEventHubProcessor;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import org.apache.http.entity.ContentType;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


public class DeleteStatusEventHubProcessor extends BaseEventHubProcessor<DeleteStatusMessage> implements Processor<DeleteStatusMessage,
        CompletableFuture<Void>> {
    private static final String CONNECTION_STRING = System.getenv(Constants.DELETE_STATUS_EVENT_HUB_CONNECTION_STRING_PROP);

    protected DeleteStatusEventHubProcessor(CompletableFuture<EventHubClient> eventHubCreationFuture) {
        super(eventHubCreationFuture);
    }

    public DeleteStatusEventHubProcessor() {
        this(new EventHubClientFactory().getEhClient(CONNECTION_STRING));
    }

    @Override
    protected List<EventData> createEventData(DeleteStatusMessage message) {
        List<EventData> eventDataList = new ArrayList<>();
        try {
            eventDataList.add(convertToEventData(message));
        } catch (IllegalArgumentException e) { //Event Data create throws IllegalArgumentException
            throw new DeleteTimeSeriesException("Error while creating Delete Time Series Status Message", e, DeleteTimeSeriesErrorType.INVALID_MESSAGE,
                    IdentifierUtil.getIdentifier("eventId", message.getEventId()), false);
        }
        return eventDataList;
    }

    @VisibleForTesting
    EventData convertToEventData(DeleteStatusMessage deleteStatusMessage) throws IllegalStateException {

        DeleteStatusData deleteStatusData = new DeleteStatusData();
        deleteStatusData.setStatus(deleteStatusMessage.getStatus());
        deleteStatusData.setEventId(deleteStatusMessage.getEventId());
        deleteStatusData.setError(deleteStatusMessage.getError());
        SapIoTAbstractionExtension inComingEventId = SapIoTAbstractionExtension.builder()
                .correlationId(deleteStatusMessage.getCorrelationId())
                .build();

        CloudEventImpl<DeleteStatusData> deleteStatusCloudEvent = CloudEventBuilder.<DeleteStatusData>builder()
                .withType("com.sap.iot.abstraction.timeseries.status.v1")
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("sap-iot-abstraction"))
                .withTime(ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
                .withDataContentType(ContentType.APPLICATION_JSON.getMimeType())
                .withExtension(new SapIoTAbstractionExtension.Format(inComingEventId))
                .withData(deleteStatusData)
                .build();

        return EventData.create(io.cloudevents.json.Json.binaryEncode(deleteStatusCloudEvent));
    }

    /**
     * Send a delete status message to the Event Hub configured through the enviroment variable name
     * {@link Constants#DELETE_STATUS_EVENT_HUB_CONNECTION_STRING_PROP}.
     *
     * @param deleteStatusMessageEntry delete status message object
     * @return completable future from sending status message to Event Hub
     * @throws IoTRuntimeException exception in Event Hub interaction
     */
    @Override
    public CompletableFuture<Void> process(DeleteStatusMessage deleteStatusMessageEntry) throws IoTRuntimeException {
        return super.process(deleteStatusMessageEntry, deleteStatusMessageEntry.getStructureId());
    }
}
