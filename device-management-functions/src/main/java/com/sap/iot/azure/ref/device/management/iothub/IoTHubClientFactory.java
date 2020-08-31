package com.sap.iot.azure.ref.device.management.iothub;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.sdk.iot.service.RegistryManager;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementErrorType;
import com.sap.iot.azure.ref.device.management.exception.DeviceManagementException;
import com.sap.iot.azure.ref.device.management.util.Constants;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;

import java.io.IOException;

class IoTHubClientFactory {

    private static RegistryManager registryManager;

    synchronized RegistryManager getIoTHubClient() {
        if (registryManager == null) {
            try {
                registryManager =  createRegistryManager();
                registerClientShutdown(registryManager);
            } catch (IllegalArgumentException ex) {
                throw new DeviceManagementException("Invalid connection string provided for IoT Hub", ex, DeviceManagementErrorType.IOTHUB_ERROR,
                        IdentifierUtil.empty(), false);
            } catch(IOException ex) {
                // requires manual restart with providing new connection string in the KeyVault
                // todo: will be enhanced with circuit-break implementation in next release
                throw new DeviceManagementException("Error in initializing connection to IoT Hub Registry Manager", ex, DeviceManagementErrorType.IOTHUB_ERROR,
                        IdentifierUtil.empty(), false);
            }
        }

        return registryManager;
    }

    private void registerClientShutdown(RegistryManager registryManager) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownClient(registryManager)));
    }

    private void shutdownClient(RegistryManager registryManager) {
        registryManager.close();
    }

    @VisibleForTesting
    RegistryManager createRegistryManager() throws IOException {
        return RegistryManager.createFromConnectionString(System.getenv(Constants.IOTHUB_REGISTRY_CONNECTION_STRING_PROP));
    }
}
