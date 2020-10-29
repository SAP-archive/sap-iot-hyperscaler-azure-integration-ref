package com.sap.iot.azure.ref.ingestion;

import com.google.common.collect.Maps;
import com.sap.iot.azure.ref.ingestion.device.mapping.DevicePayloadMapper;
import com.sap.iot.azure.ref.ingestion.model.device.mapping.DeviceMessage;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasure;
import com.sap.iot.azure.ref.ingestion.output.ADXEventHubProcessor;
import com.sap.iot.azure.ref.ingestion.output.ProcessedTimeSeriesEventHubProcessor;
import com.sap.iot.azure.ref.ingestion.processing.DeviceToProcessedMessageProcessor;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MappingFunctionTest {


    @Mock
    private ProcessedTimeSeriesEventHubProcessor processedTimeSeriesEventHubProcessor;
    @Mock
    private ADXEventHubProcessor adxEventHubProcessor;
    @Mock
    private DeviceToProcessedMessageProcessor deviceToProcessedMessageProcessor;
    @Mock
    private DevicePayloadMapper devicePayloadMapper;

    private MappingFunction mappingFunction;

    @Before
    public void prepare() {
        doReturn(Collections.singletonList(new DeviceMeasure())).when(devicePayloadMapper).apply(any(DeviceMessage.class));
        doReturn(getSampleProcessedMessages()).when(deviceToProcessedMessageProcessor).apply(any());
        doReturn(CompletableFuture.completedFuture(null)).when(processedTimeSeriesEventHubProcessor).apply(any());
        doReturn(CompletableFuture.completedFuture(null)).when(adxEventHubProcessor).apply(any());

        mappingFunction = Mockito.spy(new MappingFunction(
                processedTimeSeriesEventHubProcessor,
                adxEventHubProcessor,
                deviceToProcessedMessageProcessor
        ));

        doReturn(devicePayloadMapper).when(mappingFunction).getDevicePayloadToRawMessageMapper();

        InvocationContextTestUtil.initInvocationContext();
    }

    @Test
    public void testHttpTriggerJava() {
        mappingFunction.run(getSampleMessages(), InvocationContextTestUtil.createSystemPropertiesMap(), InvocationContextTestUtil.createPartitionContext(),
                InvocationContextTestUtil.getMockContext());

        verify(processedTimeSeriesEventHubProcessor, times(1)).apply(any());
        verify(adxEventHubProcessor, times(1)).apply(any());
    }

    private List<String> getSampleMessages() {
        return Collections.singletonList("");
    }

    private Map.Entry<String, List<ProcessedMessage>> getSampleProcessedMessages() {
        List<ProcessedMessage> sampleMessages = Collections.singletonList(new ProcessedMessage());

        return Maps.immutableEntry("", sampleMessages);
    }
}
