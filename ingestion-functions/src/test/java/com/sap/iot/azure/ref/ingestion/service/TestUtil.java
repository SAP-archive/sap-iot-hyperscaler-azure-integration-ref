package com.sap.iot.azure.ref.ingestion.service;

import com.sap.iot.azure.ref.ingestion.avro.TestAVROSchemaConstants;
import com.sap.iot.azure.ref.integration.commons.avro.AvroHelper;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtil {

    public static List<byte[]> avroMessage(int numOfMessages) {
        List<byte[]> avroMsgs = new ArrayList<>();
        for (int i = 0; i < numOfMessages; i++) {
            ProcessedMessage sampleMessage = getSampleMessage();
            avroMsgs.add(AvroHelper.serializeJsonToAvro(sampleMessage, AVRO_SCHEMA));
        }

        return avroMsgs;
    }

    public static byte[] avroMessageByte(){
        ProcessedMessage sampleMessage = getSampleMessage();
        return AvroHelper.serializeJsonToAvro(sampleMessage, AVRO_SCHEMA);
    }

    public static byte[] avroMessageByteMultipleDataTypes(){
        ProcessedMessage sampleMessage = getSampleMessageMultipleDataTypes();
        return AvroHelper.serializeJsonToAvro(sampleMessage, TestAVROSchemaConstants.ALLTYPES_AVRO_SCHEMA);
    }

    public static ProcessedMessage getSampleMessage() {

        Map<String, String> tags = new HashMap<>();
        tags.put("equipmentId", "eq1");
        tags.put("templateId", "tl1");
        tags.put("modelId","m1");
        tags.put("indicatorGroupId","ig1");

        Map<String, Object> measures = new HashMap<>();
        measures.put("_time", "2019-04-09T22:58:28.805Z");
        measures.put("ax", 10);
        measures.put("ay", 20);
        measures.put("az", 30);

        List<Map<String, Object>> measuresList = new ArrayList<>();
        measuresList.add(measures);

        return ProcessedMessage.builder()
                .sourceId("S1")
                .tags(tags)
                .measures(measuresList)
                .build();
    }

    public static ProcessedMessage getSampleMessageMultipleDataTypes() {

        Map<String, String> tags = new HashMap<>();
        tags.put("equipmentId", "string");
        tags.put("templateId", "string");
        tags.put("modelId", "string");
        tags.put("indicatorGroupId", "string");

        Map<String, Object> measures = new HashMap<>();
        measures.put("_time", "2019-04-09T22:58:28.805Z");
        measures.put("NumericFlexible", 1.1f);
        measures.put("Boolean", true);
        measures.put("String", "Some String");
        measures.put("Date", "2019-04-09T22:58:28.805Z");
        measures.put("Timestamp", 123L);
        measures.put("DateTime", "2019-04-09T22:58:28.805Z");
        measures.put("JSON", "{}");
        measures.put("Int", 123);
        measures.put("LargeString", null);
        measures.put("Long", 123L);
        measures.put("Double", 123.4);

        List<Map<String, Object>> measuresList = new ArrayList<>();
        measuresList.add(measures);

        return ProcessedMessage.builder()
                .sourceId("S1")
                .tags(tags)
                .measures(measuresList)
                .build();
    }

    public static List<ProcessedMessage> getProcessedMessageList() {
        List<ProcessedMessage> processedMessages = new ArrayList<>();
        processedMessages.add(getSampleMessage());
        return processedMessages;
    }

    public static final String AVRO_SCHEMA = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"IG1\",\n" +
            "  \"gdprDataCategory\": \"\",\n" +
            "  \"structureId\": \"IG1\",\n" +
            "  \"tenant\": \"tenant-guid-1\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"messageId\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"identifier\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"tags\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"items\": {\n" +
            "          \"type\": \"record\",\n" +
            "          \"name\": \"queryParams\",\n" +
            "          \"fields\": [\n" +
            "            {\n" +
            "              \"name\": \"modelId\",\n" +
            "              \"type\": [\n" +
            "                \"null\",\n" +
            "                \"string\"\n" +
            "              ],\n" +
            "              \"default\": null\n" +
            "            },\n" +
            "            {\n" +
            "              \"name\": \"equipmentId\",\n" +
            "              \"type\": [\n" +
            "                \"null\",\n" +
            "                \"string\"\n" +
            "              ],\n" +
            "              \"default\": null\n" +
            "            },\n" +
            "            {\n" +
            "              \"name\": \"indicatorGroupId\",\n" +
            "              \"type\": [\n" +
            "                \"null\",\n" +
            "                \"string\"\n" +
            "              ],\n" +
            "              \"default\": null\n" +
            "            },\n" +
            "            {\n" +
            "              \"name\": \"templateId\",\n" +
            "              \"type\": [\n" +
            "                \"null\",\n" +
            "                \"string\"\n" +
            "              ],\n" +
            "              \"default\": null\n" +
            "            }\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"measurements\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"items\": {\n" +
            "          \"type\": \"record\",\n" +
            "          \"name\": \"timeseriesRecord\",\n" +
            "          \"fields\": [\n" +
            "            {\n" +
            "              \"name\": \"_time\",\n" +
            "              \"type\": {\n" +
            "                \"type\": \"long\",\n" +
            "                \"logicalType\": \"nTimestamp\"\n" +
            "              }\n" +
            "            },\n" +
            "            {\n" +
            "              \"name\": \"ax\",\n" +
            "              \"type\": [\n" +
            "                \"null\",\n" +
            "                \"int\"\n" +
            "              ]\n" +
            "            },\n" +
            "            {\n" +
            "              \"name\": \"ay\",\n" +
            "              \"type\": [\n" +
            "                \"null\",\n" +
            "                \"int\"\n" +
            "              ]\n" +
            "            },\n" +
            "            {\n" +
            "              \"name\": \"az\",\n" +
            "              \"type\": [\n" +
            "                \"null\",\n" +
            "                \"int\"\n" +
            "              ]\n" +
            "            }\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    public static final String GENERIC_JSON = "{\n" +
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

    public static final String GENERIC_JSON_WITH_MULTIPLE_TAGS = "{\n" +
            "  \"messageId\": \"S1/IG1/1587023647403\",\n" +
            "  \"identifier\": \"S1\",\n" +
            "  \"tags\": [\n" +
            "    {\n" +
            "      \"modelId\": \"m1\",\n" +
            "      \"equipmentId\": \"eq1\",\n" +
            "      \"indicatorGroupId\": \"ig1\",\n" +
            "      \"templateId\": \"tl1\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"modelId\": \"m2\",\n" +
            "      \"equipmentId\": \"eq2\",\n" +
            "      \"indicatorGroupId\": \"ig2\",\n" +
            "      \"templateId\": \"tl2\"\n" +
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

    public static final String GENERIC_JSON_WITH_NULL_TAGS = "{\n" +
            "  \"messageId\": \"S1/IG1/1587023647403\",\n" +
            "  \"identifier\": \"S1\",\n" +
            "  \"tags\": null,\n" +
            "  \"measurements\": [\n" +
            "    {\n" +
            "      \"_time\": 1587023647403,\n" +
            "      \"ax\": 10,\n" +
            "      \"ay\": 20,\n" +
            "      \"az\": 30\n" +
            "    }\n" +
            "  ]\n" +
            "}";

}