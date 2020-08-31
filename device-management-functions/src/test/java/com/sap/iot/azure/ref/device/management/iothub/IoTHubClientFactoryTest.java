package com.sap.iot.azure.ref.device.management.iothub;

import com.microsoft.azure.sdk.iot.service.RegistryManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class IoTHubClientFactoryTest {

	@Mock
	private static RegistryManager registryManager;

	@Test
	public void getIoTHubClient() throws IOException {
		IoTHubClientFactory ioTHubClientFactory = Mockito.spy(IoTHubClientFactory.class);
		doReturn(registryManager).when(ioTHubClientFactory).createRegistryManager();

		ioTHubClientFactory.getIoTHubClient();
		ioTHubClientFactory.getIoTHubClient();

		verify(ioTHubClientFactory, times(1)).createRegistryManager();
	}

}