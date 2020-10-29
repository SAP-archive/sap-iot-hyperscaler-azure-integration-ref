package com.sap.iot.azure.ref.ingestion.service;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class AvroMessageServiceTest {

    @Mock
    ProcessMessageService processedMessageService;

    @InjectMocks
    private AvroMessageService avroMessageService;

    @Before
    public void setup() {
        avroMessageService = new AvroMessageService(processedMessageService);
    }

    @Test
    public void createProcessedMessageTest() {

        Pair<String, ProcessedMessageContainer> pair = ImmutablePair.of("S1", new ProcessedMessageContainer("IG1", TestUtil.getProcessedMessageList()));
        Mockito.doReturn(pair).when(processedMessageService).apply(Mockito.any());

        Map<String, Object>[] systemPropertiesMap = new HashMap[2];
        systemPropertiesMap[0] = InvocationContextTestUtil.createSystemPropertiesMap()[0];
        systemPropertiesMap[0] = InvocationContextTestUtil.createSystemPropertiesMap()[0];

        Map<String, ProcessedMessageContainer> processedMessagesMap = avroMessageService.createProcessedMessage(TestUtil.avroMessage(2), systemPropertiesMap);

        assertTrue(processedMessagesMap.containsKey("S1"));
        assertEquals(2, processedMessagesMap.get("S1").getProcessedMessages().size());

        for (Map.Entry<String, ProcessedMessageContainer> entry : processedMessagesMap.entrySet()) {
            List<ProcessedMessage> processedMessages = entry.getValue().getProcessedMessages();
            assertEquals("S1", processedMessages.get(0).getSourceId());
            assertEquals("IG1", entry.getValue().getStructureId());
        }
    }

    @Test
    public void createProcessedMessageWithNullAvroMessageTest() {

        // Branching condition case to handle if the pair is null
        Mockito.doReturn(null).when(processedMessageService).apply(Mockito.any());
        Map<String, ProcessedMessageContainer> processedMessagesMap = avroMessageService.createProcessedMessage(TestUtil.avroMessage(1),
                InvocationContextTestUtil.createSystemPropertiesMap());
        Assert.assertTrue(processedMessagesMap.isEmpty());
    }
}