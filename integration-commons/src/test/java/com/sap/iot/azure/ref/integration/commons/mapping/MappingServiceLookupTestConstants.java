package com.sap.iot.azure.ref.integration.commons.mapping;

public class MappingServiceLookupTestConstants {
    final static String SAMPLE_TENANT = "TENANT";
    final static String SAMPLE_SENSOR_ID = "sampleSensorId";
    final static String SAMPLE_SOURCE_ID = "sampleSourceId";
    final static String SAMPLE_STRUCTURE_ID_1 = "sampleStructureId1";
    final static String SAMPLE_STRUCTURE_ID_2 = "sampleStructureId2";
    final static String SAMPLE_TAG_VALUE_1 = "tagValue1";
    final static String SAMPLE_TAG_VALUE_2 = "tagValue2";
    final static String SAMPLE_TAG_SEMANTIC_1 = "tagSemantic1";
    final static String SAMPLE_TAG_SEMANTIC_2 = "tagSemantic2";
    final static String SAMPLE_MAPPING_ID = "sampleMappingId";
    final static String SAMPLE_CAPABILITY_ID_1 = "sampleCapabilityId1";
    final static String SAMPLE_CAPABILITY_ID_2 = "sampleCapabilityId2";
    final static String SAMPLE_STRUCTURE_PROPERTY_ID_1 = "sampleStructurePropertyId1";
    final static String SAMPLE_STRUCTURE_PROPERTY_ID_2 = "sampleStructurePropertyId2";
    final static String SAMPLE_CAPABILITY_PROPERTY_ID_1 = "sampleCapabilityPropertyId1";
    final static String SAMPLE_CAPABILITY_PROPERTY_ID_2 = "sampleCapabilityPropertyId2";

    final static String SAMPLE_TAG_PAYLOAD = String.format("{\n" +
                    "    \"sourceId\": \"%s\",\n" +
                    "    \"structureId\": \"%s\",\n" +
                    "    \"tag\": [\n" +
                    "        {\n" +
                    "            \"tagValue\": \"%s\",\n" +
                    "            \"tagSemantic\": \"%s\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"tagValue\": \"%s\",\n" +
                    "            \"tagSemantic\": \"%s\"\n" +
                    "        }\n" +
                    "    ]\n" +
                    "}",
            SAMPLE_SOURCE_ID,
            SAMPLE_STRUCTURE_ID_1,
            SAMPLE_TAG_VALUE_1,
            SAMPLE_TAG_SEMANTIC_1,
            SAMPLE_TAG_VALUE_2,
            SAMPLE_TAG_SEMANTIC_2);


    final static String SAMPLE_ASSIGNMENT_PAYLOAD = String.format("{\n" +
                    "    \"d\": {\n" +
                    "        \"results\": [\n" +
                    "            {\n" +
                    "                \"MappingId\": \"%s\",\n" +
                    "                \"Sensors\": {\n" +
                    "                   \"results\": [\n" +
                    "                       {\n" +
                    "                           \"SensorId\": \"%s\"\n" +
                    "                       }\n" +
                    "                   ]\n" +
                    "               }\n" +
                    "            }\n" +
                    "        ]\n" +
                    "    }\n" +
                    "}",
            SAMPLE_MAPPING_ID,
            SAMPLE_SENSOR_ID);


    final static String SAMPLE_MAPPING_PAYLOAD = String.format("{\n" +
                    "    \"d\": {\n" +
                    "        \"Measures\": {\n" +
                    "            \"results\": [\n" +
                    "                {\n" +
                    "                    \"StructureId\": \"%s\",\n" +
                    "                    \"CapabilityId\": \"%s\",\n" +
                    "                    \"PropertyMeasures\": {\n" +
                    "                        \"results\": [\n" +
                    "                            {\n" +
                    "                                \"StructurePropertyId\": \"%s\",\n" +
                    "                                \"CapabilityPropertyId\": \"%s\"\n" +
                    "                            }\n" +
                    "                        ]\n" +
                    "                    }\n" +
                    "                },\n" +
                    "                {\n" +
                    "                    \"StructureId\": \"%s\",\n" +
                    "                    \"CapabilityId\": \"%s\",\n" +
                    "                    \"PropertyMeasures\": {\n" +
                    "                        \"results\": [\n" +
                    "                            {\n" +
                    "                                \"StructurePropertyId\": \"%s\",\n" +
                    "                                \"CapabilityPropertyId\": \"%s\"\n" +
                    "                            }\n" +
                    "                        ]\n" +
                    "                    }\n" +
                    "                }\n" +
                    "            ]\n" +
                    "        }\n" +
                    "    }\n" +
                    "}",
            SAMPLE_STRUCTURE_ID_1,
            SAMPLE_CAPABILITY_ID_1,
            SAMPLE_STRUCTURE_PROPERTY_ID_1,
            SAMPLE_CAPABILITY_PROPERTY_ID_1,
            SAMPLE_STRUCTURE_ID_2,
            SAMPLE_CAPABILITY_ID_2,
            SAMPLE_STRUCTURE_PROPERTY_ID_2,
            SAMPLE_CAPABILITY_PROPERTY_ID_2);

    final static String SAMPLE_SCHEMA_PAYLOAD = "TESTSCHEMA";
}
