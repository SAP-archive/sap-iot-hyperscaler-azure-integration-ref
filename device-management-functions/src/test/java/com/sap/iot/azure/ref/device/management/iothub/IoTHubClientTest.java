package com.sap.iot.azure.ref.device.management.iothub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.sdk.iot.service.Device;
import com.microsoft.azure.sdk.iot.service.RegistryManager;
import com.microsoft.azure.sdk.iot.service.exceptions.*;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementErrorType;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementException;
import com.sap.iot.azure.ref.device.management.model.DeviceInfo;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import io.cloudevents.Attributes;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class IoTHubClientTest {

	private static final ObjectMapper objMapper = new ObjectMapper();

	@Mock
	private RegistryManager registryManager;

	@InjectMocks
	private IoTHubClient ioTHubClient;

	@Before
	public void prepare() {
		reset(registryManager);
		ioTHubClient = new IoTHubClient( registryManager );
	}

	public DeviceInfo getGenericDeviceInfoObject(String resourceStreamFile) throws IOException {
		JsonNode jsonNode = objMapper.readTree(this.getClass().getResourceAsStream(resourceStreamFile));
		byte[] deviceManagementMessage = objMapper.writeValueAsBytes(jsonNode);
		CloudEvent<Attributes, DeviceInfo> deviceManagementEvent = Json.binaryDecodeValue(deviceManagementMessage, CloudEventImpl.class, DeviceInfo.class);
		DeviceInfo deviceInfo = deviceManagementEvent.getData().orElseThrow(() -> new DeviceManagementException("device information not available",
				DeviceManagementErrorType.INVALID_CLOUD_EVENT_MESSAGE, IdentifierUtil.getIdentifier("CloudEventId", deviceManagementEvent.getAttributes().getId()), false));
		return deviceInfo;
	}

	@Test
	public void addDevice() throws IOException, IotHubException, ExecutionException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-create-payload.json");
		Mockito.when(registryManager.addDeviceAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
		ioTHubClient.addDevice(deviceInfo).get();
		verify(registryManager, times(1)).addDeviceAsync(any());
	}

	@Test
	public void updateDevice() throws IOException, IotHubException, ExecutionException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-update-payload.json");
		Mockito.when(registryManager.getDeviceAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
		ioTHubClient.updateDevice(deviceInfo).get();
		verify(registryManager, times(1)).getDeviceAsync(deviceInfo.getDeviceId());
	}

	@Test
	public void deleteDevice() throws IOException, IotHubException, ExecutionException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-delete-payload.json");
		Mockito.when(registryManager.removeDeviceAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
		ioTHubClient.deleteDevice(deviceInfo).get();
		verify(registryManager, times(1)).removeDeviceAsync(deviceInfo.getDeviceId());
	}

	@Test
	public void addDeviceWithIotHubException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-create-payload.json");
		CompletableFuture<Device> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubException("IoT Hub Exception ..."));
		Mockito.when(registryManager.addDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.addDevice(deviceInfo).get();
			fail("An expected IotHubException should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("IoT Hub Exception"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubException);
		}
		verify(registryManager, times(5)).addDeviceAsync(any());
	}

	@Test
	public void addDeviceWithIotHubNotFoundException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-create-payload.json");
		CompletableFuture<Device> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubNotFoundException("IoT Hub Not Found Exception ..."));
		Mockito.when(registryManager.addDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.addDevice(deviceInfo).get();
			fail("An expected IotHubNotFoundException should have been thrown.");
		}
		catch (ExecutionException ex) {
 			assertTrue(ex.getMessage().contains("IoT Hub Not Found Exception"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubNotFoundException);
		}
		verify(registryManager, times(1)).addDeviceAsync(any());
	}

	@Test
	public void addDeviceWithIotHubConflictException() throws IOException, IotHubException, ExecutionException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-create-payload.json");
		CompletableFuture<Device> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubConflictException("IoT Hub Conflict Exception ..."));
		Mockito.when(registryManager.addDeviceAsync(any())).thenReturn(future);
		ioTHubClient.addDevice(deviceInfo).get();
		verify(registryManager, times(1)).addDeviceAsync(any());
	}

	@Test
	public void addDeviceWithIotHubBadFormatException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-create-payload.json");
		CompletableFuture<Device> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubBadFormatException("IoT Hub Bad Format Exception ..."));
		Mockito.when(registryManager.addDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.addDevice(deviceInfo).get();
			fail("An expected IotHubBadFormatException should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("IoT Hub Bad Format Exception"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubBadFormatException);
		}
		verify(registryManager, times(1)).addDeviceAsync(any());
	}

	@Test
	public void addDeviceWithIotHubUnathorizedException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-create-payload.json");
		CompletableFuture<Device> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubUnathorizedException("IoT Hub Unauthorized Exception ..."));
		Mockito.when(registryManager.addDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.addDevice(deviceInfo).get();
			fail("An expected IotHubUnauthorizedException should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("Invalid credential to access to IoT Hub"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubUnathorizedException);
		}
		verify(registryManager, times(1)).addDeviceAsync(any());
	}

	@Test
	public void addDeviceWithIotHubTooManyDevicesException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-create-payload.json");
		CompletableFuture<Device> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubTooManyDevicesException("IoT Hub Too Many Devices Exception ..."));
		Mockito.when(registryManager.addDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.addDevice(deviceInfo).get();
			fail("An expected IoT Hub Too Many Devices Exception should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("IoT Hub Too Many Devices Exception"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubTooManyDevicesException);
		}
		verify(registryManager, times(1)).addDeviceAsync(any());
	}

	@Test
	public void addDeviceWithIotHubPreconditionFailedException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-create-payload.json");
		CompletableFuture<Device> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubPreconditionFailedException("IoT Hub Precondition Failed Exception ..."));
		Mockito.when(registryManager.addDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.addDevice(deviceInfo).get();
			fail("An expected IoT Hub Precondition Failed Exception should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("IoT Hub Precondition Failed Exception"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubPreconditionFailedException);
		}
		verify(registryManager, times(1)).addDeviceAsync(any());
	}

	@Test
	public void deleteDeviceWithIotHubException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-delete-payload.json");
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubException("IoT Hub Exception ..."));
		Mockito.when(registryManager.removeDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.deleteDevice(deviceInfo).get();
			fail("An expected IotHubException should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("IoT Hub Exception"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubException);
		}
		verify(registryManager, times(5)).removeDeviceAsync(any());
	}

	@Test
	public void deleteDeviceWithIotHubNotFoundException() throws IOException, IotHubException, InterruptedException, ExecutionException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-delete-payload.json");
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubNotFoundException("IoT Hub Not Found Exception ..."));
		Mockito.when(registryManager.removeDeviceAsync(deviceInfo.getDeviceId())).thenReturn(future);
		ioTHubClient.deleteDevice(deviceInfo).get();
		verify(registryManager, times(1)).removeDeviceAsync(any());
	}

	@Test
	public void deleteDeviceWithIotHubBadFormatException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-delete-payload.json");
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubBadFormatException("IoT Hub Bad Format Exception ..."));
		Mockito.when(registryManager.removeDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.deleteDevice(deviceInfo).get();
			fail("An expected IotHubBadFormatException should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("IoT Hub Bad Format Exception"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubBadFormatException);
		}
		verify(registryManager, times(1)).removeDeviceAsync(any());
	}

	@Test
	public void deleteDeviceWithIotHubUnathorizedException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-delete-payload.json");
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubUnathorizedException("IoT Hub Unauthorized Exception ..."));
		Mockito.when(registryManager.removeDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.deleteDevice(deviceInfo).get();
			fail("An expected IotHubUnauthorizedException should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("Invalid credential to access to IoT Hub"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubUnathorizedException);
		}
		verify(registryManager, times(1)).removeDeviceAsync(any());
	}

	@Test
	public void deleteDeviceWithIotHubTooManyDevicesException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-delete-payload.json");
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubTooManyDevicesException("IoT Hub Too Many Devices Exception ..."));
		Mockito.when(registryManager.removeDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.deleteDevice(deviceInfo).get();
			fail("An expected IoT Hub Too Many Devices Exception should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("IoT Hub Too Many Devices Exception"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubTooManyDevicesException);
		}
		verify(registryManager, times(1)).removeDeviceAsync(any());
	}

	@Test
	public void deleteDeviceWithIotHubPreconditionFailedException() throws IOException, IotHubException, InterruptedException {
		DeviceInfo deviceInfo = getGenericDeviceInfoObject("/device-management-delete-payload.json");
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		future.completeExceptionally(new IotHubPreconditionFailedException("IoT Hub Precondition Failed Exception ..."));
		Mockito.when(registryManager.removeDeviceAsync(any())).thenReturn(future);
		try {
			ioTHubClient.deleteDevice(deviceInfo).get();
			fail("An expected IoT Hub Precondition Failed Exception should have been thrown.");
		}
		catch (ExecutionException ex) {
			assertTrue(ex.getMessage().contains("IoT Hub Precondition Failed Exception"));
			assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class)) instanceof IotHubPreconditionFailedException);
		}
		verify(registryManager, times(1)).removeDeviceAsync(any());
	}

}