package com.sap.iot.azure.ref.ingestion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.ingestion.util.AvroMessageConverter;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class ProcessMessageServiceTest {

    @Mock
    AvroMessageConverter avroMessageConverter;

    @InjectMocks
    private ProcessMessageService processMessageService;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setup() {
        processMessageService = new ProcessMessageService(avroMessageConverter);
    }

    @Test
    public void createProcessedMessage() throws IOException {
        JsonNode genericMessageJSON = mapper.readTree(TestUtil.GENERIC_JSON);
        List<JsonNode> listMessages = new LinkedList<JsonNode>();
        listMessages.add(genericMessageJSON);
        Mockito.doReturn(listMessages).when(avroMessageConverter).deserializeAvroMessage(any(), any());
        Pair<byte[], Map<String, Object>> pair = Pair.of(TestUtil.avroMessageByte(), InvocationContextTestUtil.createSystemPropertiesMap("S1/IG1")[0]);
        Pair<String, List<ProcessedMessage>> processedMessagesPair = processMessageService.process(pair);
        Assert.assertEquals("S1", processedMessagesPair.getKey());
        Assert.assertEquals("S1", processedMessagesPair.getValue().get(0).getSourceId());
        Assert.assertEquals("IG1", processedMessagesPair.getValue().get(0).getStructureId());
    }

    @Test
    public void createProcessedMessageWithMultipleTags() throws IOException {
        JsonNode genericMessageJSON = mapper.readTree(TestUtil.GENERIC_JSON_WITH_MULTIPLE_TAGS);
        List<JsonNode> listMessages = new LinkedList<JsonNode>();
        listMessages.add(genericMessageJSON);
        Mockito.doReturn(listMessages).when(avroMessageConverter).deserializeAvroMessage(any(), any());
        expectedException.expect(IoTRuntimeException.class);
        expectedException.expectMessage("Multiple tags are provided for the same Source Id");
        Pair<byte[], Map<String, Object>> pair = Pair.of(TestUtil.avroMessageByte(), InvocationContextTestUtil.createSystemPropertiesMap("S1/IG1")[0]);
        processMessageService.process(pair);
    }

    @Test
    public void createProcessedMessageWithNullTags() throws IOException {
        JsonNode genericMessageJSON = mapper.readTree(TestUtil.GENERIC_JSON_WITH_NULL_TAGS);
        List<JsonNode> listMessages = new LinkedList<JsonNode>();
        listMessages.add(genericMessageJSON);
        Mockito.doReturn(listMessages).when(avroMessageConverter).deserializeAvroMessage(any(), any());
        Pair<byte[], Map<String, Object>> pair = Pair.of(TestUtil.avroMessageByte(), InvocationContextTestUtil.createSystemPropertiesMap("S1/IG1")[0]);
        Pair<String, List<ProcessedMessage>> processedMessagesPair = processMessageService.process(pair);
        Assert.assertEquals(null, processedMessagesPair.getValue().get(0).getTags());
        Assert.assertEquals("S1", processedMessagesPair.getValue().get(0).getSourceId());
        Assert.assertEquals("IG1", processedMessagesPair.getValue().get(0).getStructureId());
    }

    @Test
    public void createProcessedMessageWithIncorrectPartitionKey() throws IOException {
        JsonNode genericMessageJSON = mapper.readTree(TestUtil.GENERIC_JSON);
        List<JsonNode> listMessages = new LinkedList<JsonNode>();
        listMessages.add(genericMessageJSON);
        expectedException.expect(IoTRuntimeException.class);
        expectedException.expectMessage("sourceId and structureId cannot be identified from Partition Key");
        Pair<byte[], Map<String, Object>> pair = Pair.of(TestUtil.avroMessageByte(), InvocationContextTestUtil.createSystemPropertiesMap("S1")[0]);
        processMessageService.process(pair);
    }
}