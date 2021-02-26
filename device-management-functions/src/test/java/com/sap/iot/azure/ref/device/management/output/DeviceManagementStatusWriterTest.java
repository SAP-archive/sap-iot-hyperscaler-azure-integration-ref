package com.sap.iot.azure.ref.device.management.output;

import com.microsoft.azure.eventhubs.BatchOptions;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.sap.iot.azure.ref.device.management.model.DeviceManagementStatusInfo;
import com.sap.iot.azure.ref.device.management.model.cloudevents.SAPIoTCloudEventType;
import com.sap.iot.azure.ref.integration.commons.eventhub.BaseEventHubProcessorTest;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.util.CompletableFutures;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeviceManagementStatusWriterTest {

    @Mock
    private EventHubClient eventHubClient;

    private DeviceManagementStatusWriter deviceManagementStatusWriter;

    @Before
    public void prepare() throws EventHubException {
        deviceManagementStatusWriter = new DeviceManagementStatusWriter(CompletableFuture.completedFuture(eventHubClient));
        when(eventHubClient.createBatch(any(BatchOptions.class))).thenReturn(new BaseEventHubProcessorTest.SimpleEventBatch());
        when(eventHubClient.send(any(EventDataBatch.class))).thenReturn(CompletableFutures.voidCompletedFuture());
    }

    @Test
    public void testCreateProcess() throws ExecutionException, InterruptedException {
        deviceManagementStatusWriter.process(deviceManagementStatusCreateInfo()).get();
        verify(eventHubClient, times(1)).send(any(EventDataBatch.class));
    }

    @Test
    public void testUpdateProcess() throws ExecutionException, InterruptedException {
        deviceManagementStatusWriter.process(deviceManagementStatusUpdateInfo()).get();
        verify(eventHubClient, times(1)).send(any(EventDataBatch.class));
    }

    @Test
    public void testDeleteProcess() throws ExecutionException, InterruptedException {
        deviceManagementStatusWriter.process(deviceManagementStatusDeleteInfo()).get();
        verify(eventHubClient, times(1)).send(any(EventDataBatch.class));
    }

    @Test
    public void testCreateProcessWithInvalidStatusType() {
        try {
            deviceManagementStatusWriter.process(deviceManagementStatusCreateInvalidStatusInfo()).get();
            fail("An expected DeviceManagementException should have been thrown.");
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains("No matching status type available for CloudEvent Type DEVICE_MANAGEMENT_CREATE_STATUS_V1"));
            IoTRuntimeException ioTRuntimeException = (IoTRuntimeException) ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IoTRuntimeException.class));
            assertTrue(ioTRuntimeException.getErrorType().name().contains("INVALID_CLOUD_EVENT_TYPE"));
        }
        verify(eventHubClient, times(0)).send((EventData) any(), any());
    }

    public DeviceManagementStatusInfo deviceManagementStatusCreateInfo() {
        DeviceManagementStatusInfo deviceManagementStatusInfo = DeviceManagementStatusInfo.builder()
                .deviceManagementStatus(deviceManagementStatus())
                .sourceEventSequenceNumber("0")
                .sourceEventTransactionId("a23341-cb231-bdgj1-4234-8876gksdb34")
                .sourceEventType(SAPIoTCloudEventType.DEVICE_MANAGEMENT_CREATE_V1)
                .build();
        return deviceManagementStatusInfo;
    }

    public DeviceManagementStatusInfo deviceManagementStatusUpdateInfo() {
        DeviceManagementStatusInfo deviceManagementStatusInfo = DeviceManagementStatusInfo.builder()
                .deviceManagementStatus(deviceManagementStatus())
                .sourceEventSequenceNumber("0")
                .sourceEventTransactionId("a23341-cb231-bdgj1-4234-8776gksdb12")
                .sourceEventType(SAPIoTCloudEventType.DEVICE_MANAGEMENT_UPDATE_V1)
                .build();
        return deviceManagementStatusInfo;
    }

    public DeviceManagementStatusInfo deviceManagementStatusDeleteInfo() {
        DeviceManagementStatusInfo deviceManagementStatusInfo = DeviceManagementStatusInfo.builder()
                .deviceManagementStatus(deviceManagementStatus())
                .sourceEventSequenceNumber("0")
                .sourceEventTransactionId("a23341-cb231-bdgj1-4934-8776gksdb76")
                .sourceEventType(SAPIoTCloudEventType.DEVICE_MANAGEMENT_DELETE_V1)
                .build();
        return deviceManagementStatusInfo;
    }

    public DeviceManagementStatusInfo deviceManagementStatusCreateInvalidStatusInfo() {
        DeviceManagementStatusInfo deviceManagementStatusInfo = DeviceManagementStatusInfo.builder()
                .deviceManagementStatus(deviceManagementStatus())
                .sourceEventSequenceNumber("0")
                .sourceEventTransactionId("a23341-cb231-bdgj1-4234-8876gksdb34")
                .sourceEventType(SAPIoTCloudEventType.DEVICE_MANAGEMENT_CREATE_STATUS_V1)
                .build();
        return deviceManagementStatusInfo;
    }

    public DeviceManagementStatusInfo.DeviceManagementStatus deviceManagementStatus() {
        DeviceManagementStatusInfo.DeviceManagementStatus deviceManagementStatus = DeviceManagementStatusInfo.DeviceManagementStatus.builder()
                .deviceId("d1")
                .deviceAlternateId("d1")
                .status("SUCCESS")
                .error(error())
                .build();
        return deviceManagementStatus;
    }

    public DeviceManagementStatusInfo.Error error() {
        DeviceManagementStatusInfo.Error error = DeviceManagementStatusInfo.Error.builder()
                .code("DEVICE_MANAGEMENT_SUCCESS")
                .message(null)
                .build();
        return error;
    }


}