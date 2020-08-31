package com.sap.iot.azure.ref.device.management.output;

import com.sap.iot.azure.ref.device.management.exception.DeviceManagementErrorType;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementException;
import com.sap.iot.azure.ref.device.management.model.DeviceManagementStatusInfo;
import com.sap.iot.azure.ref.device.management.model.cloudevents.SAPIoTAbstractionExtension;
import com.sap.iot.azure.ref.device.management.model.cloudevents.SAPIoTCloudEventType;
import com.sap.iot.azure.ref.device.management.util.Constants;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.connection.EventHubClientFactory;
import com.sap.iot.azure.ref.integration.commons.eventhub.BaseEventHubProcessor;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import org.apache.http.entity.ContentType;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class DeviceManagementStatusWriter extends BaseEventHubProcessor<DeviceManagementStatusInfo> implements Processor<DeviceManagementStatusInfo, CompletableFuture<Void>>{

    private static final String CONNECTION_STRING = System.getenv(Constants.DEVICE_MANAGEMENT_STATUS_CONNECTION_STRING_PROP);

    public DeviceManagementStatusWriter() {
        this(new EventHubClientFactory().getEhClient(CONNECTION_STRING));
    }

    @VisibleForTesting
    DeviceManagementStatusWriter(CompletableFuture<EventHubClient> eventHubClientFuture) {
        super(eventHubClientFuture);
    }

    @Override
    protected List<EventData> createEventData(DeviceManagementStatusInfo deviceManagementStatusInfo) {
        // prepare the cloud event message
        SAPIoTAbstractionExtension inComingTxId = SAPIoTAbstractionExtension.builder()
                .transactionId(deviceManagementStatusInfo.getSourceEventTransactionId())
                .sequenceNumber(deviceManagementStatusInfo.getSourceEventSequenceNumber())
                .build();

        CloudEventImpl<DeviceManagementStatusInfo.DeviceManagementStatus> deviceManagementStatusCloudEvent = CloudEventBuilder.<DeviceManagementStatusInfo.DeviceManagementStatus>builder()
                .withType(getMappingStatusType(deviceManagementStatusInfo.getSourceEventType()).getValue())
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("customer-managed-azure-iot"))
                .withTime(ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
                .withDataContentType(ContentType.APPLICATION_JSON.getMimeType())
                .withExtension(new SAPIoTAbstractionExtension.Format(inComingTxId))
                .withData(deviceManagementStatusInfo.getDeviceManagementStatus())
                .build();

        return Collections.singletonList(EventData.create(Json.binaryEncode(deviceManagementStatusCloudEvent)));
    }

    @Override
    public CompletableFuture<Void> process(DeviceManagementStatusInfo deviceManagementStatusInfo) throws IoTRuntimeException {
        return super.process(deviceManagementStatusInfo, deviceManagementStatusInfo.getDeviceManagementStatus().getDeviceId());
    }

    private SAPIoTCloudEventType getMappingStatusType(SAPIoTCloudEventType sourceEventType) {
        switch (sourceEventType) {
            case DEVICE_MANAGEMENT_CREATE_V1:
                return SAPIoTCloudEventType.DEVICE_MANAGEMENT_CREATE_STATUS_V1;
            case DEVICE_MANAGEMENT_UPDATE_V1:
                return SAPIoTCloudEventType.DEVICE_MANAGEMENT_UPDATE_STATUS_V1;
            case DEVICE_MANAGEMENT_DELETE_V1:
                return SAPIoTCloudEventType.DEVICE_MANAGEMENT_DELETE_STATUS_V1;
        }

        throw new DeviceManagementException(String.format("No matching status type available for CloudEvent Type %s", sourceEventType),
                DeviceManagementErrorType.INVALID_CLOUD_EVENT_TYPE, IdentifierUtil.empty(), false);
    }
}
