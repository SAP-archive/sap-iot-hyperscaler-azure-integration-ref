package com.sap.iot.azure.ref.ingestion.service;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class AvroMessageServiceTest {

    @Mock
    ProcessMessageService processedMessages;

    @InjectMocks
    private AvroMessageService avroMessageService;

    @Before
    public void setup() {
        avroMessageService = new AvroMessageService(processedMessages);
    }

    @Test
    public void createProcessedMessageTest() {

        ImmutablePair<String, List<ProcessedMessage>> pair = ImmutablePair.of("S1", TestUtil.getProcessedMessageList());
        Mockito.doReturn(pair).when(processedMessages).apply(Mockito.any());

        Map<String, List<ProcessedMessage>> processedMessagesMap = avroMessageService.createProcessedMessage(TestUtil.avroMessage(),
                InvocationContextTestUtil.createSystemPropertiesMap());

        for (Map.Entry<String, List<ProcessedMessage>> entry : processedMessagesMap.entrySet())
        {
            List<ProcessedMessage> value = entry.getValue();
            assertEquals("S1", value.get(0).getSourceId());
            assertEquals("IG1", value.get(0).getStructureId());
        }
    }

    @Test
    public void createProcessedMessageWithNullAvroMessageTest() {

        // Branching condition case to handle if the pair is null
        ImmutablePair<String, List<ProcessedMessage>> pairNull = null;
        Mockito.doReturn(pairNull).when(processedMessages).apply(Mockito.any());
        Map<String, List<ProcessedMessage>> processedMessagesMap = avroMessageService.createProcessedMessage(TestUtil.avroMessage(),
                InvocationContextTestUtil.createSystemPropertiesMap());
        Assert.assertTrue(processedMessagesMap.isEmpty());

    }
}