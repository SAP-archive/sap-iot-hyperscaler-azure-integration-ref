package com.sap.iot.azure.ref.device.management.eventhandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.sdk.iot.service.Device;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementErrorType;
import com.sap.iot.azure.ref.device.management.iothub.IoTHubClient;
import com.sap.iot.azure.ref.device.management.model.DeviceInfo;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import io.cloudevents.Attributes;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DeviceManagementEventHandlerTest {

	private static final ObjectMapper objMapper = new ObjectMapper();

	@Mock
	private IoTHubClient ioTHubClient;

	@InjectMocks
	DeviceManagementEventHandler deviceManagementEventHandler;

	@Before
	public void prepare() {
		deviceManagementEventHandler = new DeviceManagementEventHandler( ioTHubClient );
	}

	public CloudEvent<Attributes, DeviceInfo> getGenericDeviceInfoObject(String resourceStreamFile) throws IOException {
		JsonNode jsonNode = objMapper.readTree(this.getClass().getResourceAsStream(resourceStreamFile));
		byte[] deviceManagementMessage = objMapper.writeValueAsBytes(jsonNode);
		CloudEvent<Attributes, DeviceInfo> deviceManagementEvent = Json.binaryDecodeValue(deviceManagementMessage, CloudEventImpl.class, DeviceInfo.class);
		return deviceManagementEvent;
	}

	@Test
	public void testCreateDeviceProcess() throws IOException {
		CloudEvent<Attributes, DeviceInfo> deviceManagementEvent = getGenericDeviceInfoObject("/device-management-create-payload.json");
		Mockito.when(ioTHubClient.addDevice(any())).thenReturn(CompletableFuture.completedFuture(null));
		deviceManagementEventHandler.process(deviceManagementEvent);
		verify(ioTHubClient, times(1)).addDevice(any());
	}

	@Test
	public void testUpdateDeviceProcess() throws IOException {
		CloudEvent<Attributes, DeviceInfo> deviceManagementEvent = getGenericDeviceInfoObject("/device-management-update-payload.json");
		Mockito.when(ioTHubClient.updateDevice(any())).thenReturn(CompletableFuture.completedFuture(null));
		deviceManagementEventHandler.process(deviceManagementEvent);
		verify(ioTHubClient, times(1)).updateDevice(any());
	}

	@Test
	public void testDeleteDeviceProcess() throws IOException {
		CloudEvent<Attributes, DeviceInfo> deviceManagementEvent = getGenericDeviceInfoObject("/device-management-delete-payload.json");
		Mockito.when(ioTHubClient.deleteDevice(any())).thenReturn(CompletableFuture.completedFuture(null));
		deviceManagementEventHandler.process(deviceManagementEvent);
		verify(ioTHubClient, times(1)).deleteDevice(any());
	}

	@Test
	public void testInvalidDeviceMessageProcess() throws IOException {
		CloudEvent<Attributes, DeviceInfo> deviceManagementEvent = getGenericDeviceInfoObject("/device-management-invalid-status-type.json");
		try {
			deviceManagementEventHandler.process(deviceManagementEvent).get();
			fail("An expected DeviceManagementException should have been thrown.");
		}
		catch (Exception ex) {
			assertTrue(ex.getMessage().contains("Invalid message request type DEVICE_MANAGEMENT_CREATE_STATUS_V1"));
			IoTRuntimeException ioTRuntimeException = (IoTRuntimeException) ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IoTRuntimeException.class));
			assertTrue(ioTRuntimeException.getErrorType().name().contains("INVALID_CLOUD_EVENT_TYPE"));
		}
		verify(ioTHubClient, times(0)).addDevice(any());
		verify(ioTHubClient, times(0)).updateDevice(any());
		verify(ioTHubClient, times(0)).deleteDevice(any());
	}

	@Test
	public void testCreateDeviceWithExceptionProcess() throws IOException, ExecutionException, InterruptedException {
		CloudEvent<Attributes, DeviceInfo> deviceManagementEvent = getGenericDeviceInfoObject("/device-management-create-payload.json");
		CompletableFuture<Device> future = new CompletableFuture();
		future.completeExceptionally(IoTRuntimeException.wrapNonTransient(IdentifierUtil.empty(), DeviceManagementErrorType.DEVICE_MANAGEMENT_ERROR, "permanent-exception"));
		Mockito.when(ioTHubClient.addDevice(any())).thenReturn(future);
		deviceManagementEventHandler.process(deviceManagementEvent).get();
		verify(ioTHubClient, times(1)).addDevice(any());
	}

	@Test
	public void testProcessWithoutDeviceData() throws IOException {
		CloudEvent<Attributes, DeviceInfo> deviceManagementEvent = getGenericDeviceInfoObject("/device-management-without-device-data.json");
		try {
			deviceManagementEventHandler.process(deviceManagementEvent).get();
			Assert.fail("An expected DeviceManagementException should have been thrown.");
		}
		catch (Exception ex) {
			assertTrue(ex.getMessage().contains("device information not available"));
			IoTRuntimeException ioTRuntimeException = (IoTRuntimeException) ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IoTRuntimeException.class));
			assertTrue(ioTRuntimeException.getErrorType().name().contains("INVALID_CLOUD_EVENT_MESSAGE"));
		}
	}

}