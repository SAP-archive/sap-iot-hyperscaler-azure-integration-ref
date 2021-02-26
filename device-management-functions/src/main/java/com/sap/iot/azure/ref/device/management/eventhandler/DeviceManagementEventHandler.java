package com.sap.iot.azure.ref.device.management.eventhandler;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.sdk.iot.service.Device;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementErrorType;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementException;
import com.sap.iot.azure.ref.device.management.iothub.IoTHubClient;
import com.sap.iot.azure.ref.device.management.model.DeviceInfo;
import com.sap.iot.azure.ref.device.management.model.DeviceManagementStatusInfo;
import com.sap.iot.azure.ref.device.management.model.DeviceManagementStatusInfo.DeviceManagementStatus;
import com.sap.iot.azure.ref.device.management.model.cloudevents.SAPIoTAbstractionExtension;
import com.sap.iot.azure.ref.device.management.model.cloudevents.SAPIoTCloudEventType;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import io.cloudevents.Attributes;
import io.cloudevents.CloudEvent;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

public class DeviceManagementEventHandler implements Processor<CloudEvent<Attributes, DeviceInfo>, CompletableFuture<DeviceManagementStatus>> {

    private final IoTHubClient ioTHubClient;

    public DeviceManagementEventHandler() {
        this(new IoTHubClient());
    }

    @VisibleForTesting
    DeviceManagementEventHandler(IoTHubClient ioTHubClient) {
        this.ioTHubClient = ioTHubClient;
    }

    @Override
    public CompletableFuture<DeviceManagementStatus> process(CloudEvent<Attributes, DeviceInfo> deviceMgmtCloudEvent) throws DeviceManagementException {

        DeviceInfo deviceInfo = deviceMgmtCloudEvent.getData().orElseThrow(() -> new DeviceManagementException("device information not available",
                DeviceManagementErrorType.INVALID_CLOUD_EVENT_MESSAGE, IdentifierUtil.getIdentifier("CloudEventId",
                deviceMgmtCloudEvent.getAttributes().getId()), false));

        SAPIoTCloudEventType sapIoTCloudEventType = SAPIoTCloudEventType.ofValue(deviceMgmtCloudEvent.getAttributes().getType());
        CompletableFuture<Device> deviceManagementFuture;

        switch(sapIoTCloudEventType) {
            case DEVICE_MANAGEMENT_CREATE_V1:
                deviceManagementFuture = ioTHubClient.addDevice(deviceInfo);
                break;
            case DEVICE_MANAGEMENT_UPDATE_V1:
                deviceManagementFuture = ioTHubClient.updateDevice(deviceInfo);
                break;
            case DEVICE_MANAGEMENT_DELETE_V1:
                deviceManagementFuture = ioTHubClient.deleteDevice(deviceInfo);
                break;

            default:
                SAPIoTAbstractionExtension sapIoTAbstractionExtension = SAPIoTAbstractionExtension.getExtension(deviceMgmtCloudEvent);
                throw new DeviceManagementException(String.format("Invalid message request type %s", sapIoTCloudEventType),
                        DeviceManagementErrorType.INVALID_CLOUD_EVENT_TYPE, IdentifierUtil.getIdentifier("TransactionId", sapIoTAbstractionExtension.getTransactionId()),
                        false);
        }

        return deviceManagementFuture.handleAsync((deviceAlias, ex) -> {

            InvocationContext.getLogger().log(Level.INFO, "Completed processing for device " + deviceInfo.getDeviceId());
            if (ex == null) {
                // device management is successful
                return DeviceManagementStatus.builder()
                        .deviceId(deviceAlias.getDeviceId())
                        .deviceAlternateId(deviceAlias.getDeviceId())
                        .status("SUCCESS")
                        .build();
            } else {
                // log the error and exception from IoT Hub Client
                InvocationContext.getLogger().log(Level.SEVERE, "Error in processing device management request", ex);

                // write error to the status message
                String errorCode = DeviceManagementErrorType.DEVICE_MANAGEMENT_ERROR.name();
                int appExceptionIndex = ExceptionUtils.indexOfType(ex, IoTRuntimeException.class);
                if (appExceptionIndex >= 0) {
                    IoTRuntimeException ingestionRuntimeEx = (IoTRuntimeException) ExceptionUtils.getThrowableList(ex).get(appExceptionIndex);
                    errorCode = ingestionRuntimeEx.getErrorType().name();
                }

                DeviceManagementStatusInfo.Error error = DeviceManagementStatusInfo.Error.builder()
                        .code(errorCode)
                        .message(ExceptionUtils.getRootCause(ex).getMessage())
                        .build();

                return DeviceManagementStatus.builder()
                        .deviceId(deviceInfo.getDeviceId())
                        .deviceAlternateId(deviceInfo.getDeviceId())
                        .status("ERROR")
                        .error(error)
                        .build();
            }
        });
    }
}