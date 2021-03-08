package com.sap.iot.azure.ref.ingestion;

import com.google.common.collect.ImmutableMap;
import com.sap.iot.azure.ref.ingestion.output.ADXEventHubProcessor;
import com.sap.iot.azure.ref.ingestion.service.AvroMessageService;
import com.sap.iot.azure.ref.ingestion.service.TestUtil;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil.createPartitionContext;
import static com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil.createSystemPropertiesMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AvroParserFunctionTest {

    @Mock
    private AvroMessageService avroMessageService;
    @Mock
    private ADXEventHubProcessor adxEventHubProcessor;

    @InjectMocks
    AvroParserFunction avroParserFunction;

    @Before
    public void prepare() {
        Map<String, ProcessedMessageContainer> processedMessageMap = ImmutableMap.of("S1", new ProcessedMessageContainer("IG1",
                TestUtil.getProcessedMessageList()));
        doReturn(processedMessageMap).when(avroMessageService).createProcessedMessage(Mockito.any(), Mockito.any());
        avroParserFunction = new AvroParserFunction(avroMessageService, adxEventHubProcessor);
        InvocationContextTestUtil.initInvocationContext();
    }

    @Test
    public void testRun() {
        doReturn(CompletableFuture.completedFuture(null)).when(adxEventHubProcessor).apply(any());
        avroParserFunction.run(TestUtil.avroMessage(1), createSystemPropertiesMap(),
                createPartitionContext(), InvocationContextTestUtil.getMockContext());
        verify(avroMessageService, times(1)).createProcessedMessage(any(), any());
        verify(adxEventHubProcessor, times(1)).apply(any());
    }

}
