package com.sap.iot.azure.ref.ingestion.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingHelper;
import com.sap.iot.azure.ref.ingestion.service.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class AvroMessageConverterTest {

    @Mock
    MappingHelper mappingHelperMock;

    @InjectMocks
    private AvroMessageConverter avroMessageConverter;

    private static final ObjectMapper mapper = new ObjectMapper();
    private String STRUCTURE_ID = "IG1";
    private String GENERIC_JSON = "{\n" +
            "  \"messageId\": \"S1/IG1/1587023647403\",\n" +
            "  \"identifier\": \"S1\",\n" +
            "  \"tags\": [\n" +
            "    {\n" +
            "      \"modelId\": \"m1\",\n" +
            "      \"equipmentId\": \"eq1\",\n" +
            "      \"indicatorGroupId\": \"ig1\",\n" +
            "      \"templateId\": \"tl1\"\n" +
            "    }\n" +
            "  ],\n" +
            "  \"measurements\": [\n" +
            "    {\n" +
            "      \"_time\": 1587023647403,\n" +
            "      \"ax\": 10,\n" +
            "      \"ay\": 20,\n" +
            "      \"az\": 30\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @Before
    public void setup() {
        avroMessageConverter = new AvroMessageConverter(mappingHelperMock);
        Mockito.when(mappingHelperMock.getSchemaInfo(STRUCTURE_ID)).thenReturn(TestUtil.AVRO_SCHEMA);
    }

    @Test
    public void convertAvroToPOJOTest() throws IOException {

        List<JsonNode> deserializedAvroMessages = avroMessageConverter.deserializeAvroMessage(STRUCTURE_ID, TestUtil.avroMessageByte());
        Mockito.verify(mappingHelperMock, Mockito.times(1)).getSchemaInfo(STRUCTURE_ID);
        JsonNode genericMessageJSON = mapper.readTree(GENERIC_JSON);
        List<JsonNode> expectedMessages = new LinkedList<JsonNode>();
        expectedMessages.add(genericMessageJSON);

        Assert.assertEquals(1, deserializedAvroMessages.size());
        Assert.assertEquals(expectedMessages.get(0).get("identifier"), deserializedAvroMessages.get(0).get("identifier"));
        Assert.assertEquals(expectedMessages.get(0).get("tags").get("equipmentId"), deserializedAvroMessages.get(0).get("tags").get("equipmentId")) ;
        Assert.assertEquals(expectedMessages.get(0).get("measurements").get("ax"), deserializedAvroMessages.get(0).get("measurements").get("ax")) ;

        List<JsonNode> deserializedMultipleDataTypesAvroMessages = avroMessageConverter.deserializeAvroMessage(STRUCTURE_ID, TestUtil.avroMessageByteMultipleDataTypes());
        Assert.assertEquals("S1", deserializedMultipleDataTypesAvroMessages.get(0).get("identifier").textValue());
    }
}