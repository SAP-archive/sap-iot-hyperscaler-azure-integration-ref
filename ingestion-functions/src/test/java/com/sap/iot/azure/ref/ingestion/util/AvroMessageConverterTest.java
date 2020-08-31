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
    static final String STRUCTURE_ID = "IG1";
    static final String GENERIC_JSON = "{\"messageId\":\"S1/IG1/1587023647403\",\"identifier\":\"S1\",\"structureId\":\"IG1\"," +
            "\"tenant\":\"tenantId\",\"tags\":[{\"modelId\":\"m1\",\"equipmentId\":\"eq1\",\"indicatorGroupId\":\"ig1\",\"templateId\":\"tl1\"}]," +
            "\"measurements\":[{\"_time\":1587023647403,\"ax\":10,\"ay\":20,\"az\":30}]}";

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
        List<JsonNode> deserializedMultipleDataTypesAvroMessages = avroMessageConverter.deserializeAvroMessage(STRUCTURE_ID, TestUtil.avroMessageByteMultipleDataTypes());

        Assert.assertEquals(1, deserializedAvroMessages.size());
        Assert.assertEquals(expectedMessages.get(0).get("structureId"), deserializedAvroMessages.get(0).get("structureId"));
        Assert.assertEquals(expectedMessages.get(0).get("identifier"), deserializedAvroMessages.get(0).get("identifier"));
        Assert.assertEquals(expectedMessages.get(0).get("tenant"), deserializedAvroMessages.get(0).get("tenant"));
        Assert.assertEquals(expectedMessages.get(0).get("tags").get("equipmentId"), deserializedAvroMessages.get(0).get("tags").get("equipmentId")) ;
        Assert.assertEquals(expectedMessages.get(0).get("measurements").get("ax"), deserializedAvroMessages.get(0).get("measurements").get("ax")) ;
        Assert.assertEquals("IG1", deserializedMultipleDataTypesAvroMessages.get(0).get("structureId").textValue());
        Assert.assertEquals("S1", deserializedMultipleDataTypesAvroMessages.get(0).get("identifier").textValue());

    }
}