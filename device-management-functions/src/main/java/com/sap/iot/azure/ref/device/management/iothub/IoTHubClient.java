package com.sap.iot.azure.ref.device.management.iothub;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.sdk.iot.service.Device;
import com.microsoft.azure.sdk.iot.service.RegistryManager;
import com.microsoft.azure.sdk.iot.service.exceptions.*;
import com.microsoft.azure.sdk.iot.service.transport.http.HttpResponse;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementErrorType;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementException;
import com.sap.iot.azure.ref.device.management.model.DeviceInfo;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static com.sap.iot.azure.ref.device.management.util.Constants.MAX_RETRIES;

public class IoTHubClient {

	private final RegistryManager registryManager;
	private static final RetryTaskExecutor retryTaskExecutor = new RetryTaskExecutor();

	public IoTHubClient() {
		this(new IoTHubClientFactory().getIoTHubClient());
	}

	@VisibleForTesting
	IoTHubClient(RegistryManager registryManager) {
		this.registryManager = registryManager;
	}

	/**
	 * creates a device with retry in case of transient errors in connecting to IoT Hub
	 * @param deviceInfo device to be created
	 * @return future object for the created device
	 *
	 */
	public CompletableFuture<Device> addDevice(DeviceInfo deviceInfo) {

		return retryTaskExecutor.executeWithRetry(() -> {
			// can throw illegalArgException in case device id is empty
			try {
				Device device = Device.createFromId(deviceInfo.getDeviceId(), null, null);
				return registryManager.addDeviceAsync(device).handleAsync(this.applyExceptionFilter(deviceInfo));
			} catch (IllegalArgumentException ex) {
				throw new DeviceManagementException("Invalid device id",  DeviceManagementErrorType.IOTHUB_ERROR, IdentifierUtil.getIdentifier("deviceId",
						deviceInfo.getDeviceId()), false);
			}
		}, MAX_RETRIES);
	}

	/**
	 * In this version of reference implementation, the device model is not stored in Azure IoT Hub or other Azure services.
	 * So on update of device, no changes are required on the device itself
	 *
	 * @param deviceInfo device to be updated
	 * @return future object for the device
	 */
	public CompletableFuture<Device> updateDevice(DeviceInfo deviceInfo) {

		return retryTaskExecutor.executeWithRetry(() -> registryManager.getDeviceAsync(deviceInfo.getDeviceId())
				.handleAsync(this.applyExceptionFilter(deviceInfo)), MAX_RETRIES);
	}

	public CompletableFuture<Device> deleteDevice(DeviceInfo deviceInfo) {
		return retryTaskExecutor.executeWithRetry(() -> registryManager.removeDeviceAsync(deviceInfo.getDeviceId())
				.handleAsync(this.applyExceptionFilterForDelete(deviceInfo)), MAX_RETRIES);
	}

	/**
	 * handles exception thrown from RegistryManager and handles as follows
	 * 	- are skipped silently since the expected result is not affected (e.g., create device throwing ResourceExits exception
	 * 	- exceptions that are permanent type are wrapped as NonTransient exception so that {@link RetryTaskExecutor} will skip retrying in such exception
	 * 	- other exceptions are wrapped as transient type
	 *
	 *  ref: {@link IotHubExceptionManager#httpResponseVerification(HttpResponse)}
	 *  Following exceptions will not be retried:
	 *  IotHubBadFormatException, IotHubTooManyDevicesException, IotHubPreconditionFailedException, IotHubConflictException
	 *  All other exceptions subclassing IoTHubException & IOException will be retried
	 */
	private BiFunction<Device, Throwable, Device> applyExceptionFilter(DeviceInfo deviceInfo) {

		return (deviceAlias, ex) -> {
			if (ex != null) {
				ObjectNode identifier = IdentifierUtil.getIdentifier("deviceId", deviceInfo.getDeviceId());

				if (ExceptionUtils.hasCause(ex, IotHubException.class)) { // error from IoT Hub client
					IotHubException iotHubException = (IotHubException) ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class));

					if (iotHubException instanceof IotHubConflictException) {
						// since device already exists, it's treated as not an exception in this scenario
						InvocationContext.getLogger().info(String.format("Device with id %s already exists in IoT Hub with message %s", deviceInfo.getDeviceId(),
								ex.getMessage()));

						return Device.createFromId(deviceInfo.getDeviceId(), null, null);
					} else if (iotHubException instanceof IotHubBadFormatException ||
							iotHubException instanceof IotHubTooManyDevicesException ||
							iotHubException instanceof IotHubPreconditionFailedException ||
							iotHubException instanceof IotHubNotFoundException) {

						// retry is skipped in above exceptions since the error is permanent type and retrying will not help
						throw new DeviceManagementException("Error in creating / updating device - " + iotHubException.getMessage(), ex, DeviceManagementErrorType.IOTHUB_ERROR,
								identifier, false);
					} else if (iotHubException instanceof IotHubUnathorizedException) {

						// retry is skipped since the connection string is provided via KeyVault reference which need to be updated manually
						throw new DeviceManagementException("Invalid credential to access to IoT Hub - Requires restart updating the connection string in Key Vault",
								ex, DeviceManagementErrorType.IOTHUB_ERROR, identifier, false);
					} else {
						throw new DeviceManagementException("Error in creating / updating device - " + iotHubException.getMessage(), ex, DeviceManagementErrorType.IOTHUB_ERROR,
								identifier, true);
					}

				} else { // any other exception
					throw new DeviceManagementException("Error in creating / updating device - ", ex, DeviceManagementErrorType.DEVICE_MANAGEMENT_ERROR,
							identifier, true);
				}
			}

			return deviceAlias;
		};
	}

	// similar as above function with minor changes for handling specific to delete scenario
	private BiFunction<Boolean, Throwable, Device> applyExceptionFilterForDelete(DeviceInfo deviceInfo) {

		return (deletionStatus, ex) -> {

			ObjectNode identifier = IdentifierUtil.getIdentifier("deviceId", deviceInfo.getDeviceId());
			if (ex == null) { // device creation is successful
				return Device.createFromId(deviceInfo.getDeviceId(), null, null);

			} else {
				if (ExceptionUtils.hasCause(ex, IotHubException.class)) { // error from IoT Hub client

					IotHubException iotHubException = (IotHubException) ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IotHubException.class));
					if (iotHubException instanceof IotHubNotFoundException) {
						InvocationContext.getLogger().info(String.format("Device with id %s already deleted in IoT Hub with message %s", deviceInfo.getDeviceId(),
								ex.getMessage()));

						return Device.createFromId(deviceInfo.getDeviceId(), null, null);
					} else if (iotHubException instanceof IotHubBadFormatException ||
							iotHubException instanceof IotHubTooManyDevicesException ||
							iotHubException instanceof IotHubPreconditionFailedException) {

						// retry is skipped in above exceptions since the error is permanent type and retrying will not help
						throw new DeviceManagementException("Error in deleting device - " + iotHubException.getMessage(), ex, DeviceManagementErrorType.IOTHUB_ERROR,
								identifier, false);
					} else if (iotHubException instanceof IotHubUnathorizedException) {

						// retry is skipped since the connection string is provided via KeyVault reference which need to be updated manually
						throw new DeviceManagementException("Invalid credential to access to IoT Hub - Requires restart updating the connection string in Key Vault",
								ex, DeviceManagementErrorType.IOTHUB_ERROR, identifier, false);
					} else {
						throw new DeviceManagementException("Error in deleting device - " + iotHubException.getMessage(), ex, DeviceManagementErrorType.IOTHUB_ERROR,
								identifier, true);
					}
				} else {
					throw new DeviceManagementException("Error in deleting device", ex, DeviceManagementErrorType.DEVICE_MANAGEMENT_ERROR, identifier, true);
				}
			}
		};
	}
}
