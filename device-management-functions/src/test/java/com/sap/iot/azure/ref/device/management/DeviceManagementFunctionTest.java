package com.sap.iot.azure.ref.device.management;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.device.management.eventhandler.DeviceManagementEventHandler;
import com.sap.iot.azure.ref.device.management.model.DeviceManagementStatusInfo;
import com.sap.iot.azure.ref.device.management.output.DeviceManagementStatusWriter;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeviceManagementFunctionTest {

    private static final ObjectMapper objMapper = new ObjectMapper();

    @Mock
    private DeviceManagementEventHandler deviceManagementEventHandler;

    @Mock
    private DeviceManagementStatusWriter deviceManagementStatusWriter;

    private DeviceManagementFunction deviceManagementFunction;
    private final String testDeviceId = "d1";

    @BeforeClass
    public static void prepareClass() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Before
    public void prepare() {
        deviceManagementFunction = new DeviceManagementFunction( deviceManagementEventHandler, deviceManagementStatusWriter );

        DeviceManagementStatusInfo.DeviceManagementStatus testStatus = DeviceManagementStatusInfo.DeviceManagementStatus.builder()
                .deviceId(testDeviceId)
                .deviceAlternateId(testDeviceId)
                .status("SUCCESS")
                .build();

        when(deviceManagementEventHandler.apply(any())).thenReturn(CompletableFuture.completedFuture(testStatus));
        when(deviceManagementStatusWriter.apply(any(DeviceManagementStatusInfo.class))).thenReturn(CompletableFuture.completedFuture(null));
        InvocationContextTestUtil.initInvocationContext();
    }

    @Test
    public void testRun() throws IOException {
        JsonNode jsonNode = objMapper.readTree(this.getClass().getResourceAsStream("/device-management-create-payload.json"));
        deviceManagementFunction.run(Collections.singletonList(objMapper.writeValueAsBytes(jsonNode)), InvocationContextTestUtil.createSystemPropertiesMap(),
                InvocationContextTestUtil.createPartitionContext(), InvocationContextTestUtil.getMockContext());
        verify(deviceManagementStatusWriter, times(1)).apply(any(DeviceManagementStatusInfo.class));
    }

    @Test
    public void testRunWithIncorrectCloudEventMessage() throws IOException {
        JsonNode jsonNode = objMapper.readTree(this.getClass().getResourceAsStream("/device-management-invalid-payload-format.json"));
        deviceManagementFunction.run(Collections.singletonList(objMapper.writeValueAsBytes(jsonNode)), InvocationContextTestUtil.createSystemPropertiesMap(),
                InvocationContextTestUtil.createPartitionContext(), InvocationContextTestUtil.getMockContext());

        InvocationContextTestUtil.filterLog("Failed to decode: Cannot construct instance of `io.cloudevents.v1.CloudEventImpl`")
                .orElseThrow(() -> new AssertionError("Expected exception message - failed to decode message"));

        InvocationContextTestUtil.filterLog("invalid payload: 'source' must not be null")
                .orElseThrow(() -> new AssertionError("Expected exception message - invalid payload"));
    }
}