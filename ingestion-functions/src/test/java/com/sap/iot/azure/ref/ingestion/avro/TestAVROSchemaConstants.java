package com.sap.iot.azure.ref.ingestion.avro;

public class TestAVROSchemaConstants {
    public static final String SIMPLE_AVRO_SCHEMA = "{" +
            "  \"type\": \"record\"," +
            "  \"name\": \"schema\"," +
            "  \"gdprDataCategory\": \"\",\n" +
            "  \"structureId\": \"schema\",\n" +
            "  \"tenant\": \"tenant-guid-1\",\n" +
            "  \"fields\": [" +
            "    {" +
            "      \"name\": \"messageId\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"identifier\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"tags\"," +
            "      \"type\": {" +
            "        \"type\": \"array\"," +
            "        \"items\": {" +
            "          \"type\": \"record\"," +
            "          \"name\": \"queryParams\"," +
            "          \"fields\": [" +
            "            {" +
            "              \"name\": \"sampleTagKey\"," +
            "              \"type\": [" +
            "                \"null\"," +
            "                \"string\"" +
            "              ]" +
            "            }" +
            "          ]" +
            "        }" +
            "      }" +
            "    }," +
            "    {" +
            "      \"name\": \"measurements\"," +
            "      \"type\": {" +
            "        \"type\": \"array\"," +
            "        \"items\": {" +
            "          \"type\": \"record\"," +
            "          \"name\": \"timeseriesRecord\"," +
            "          \"fields\": [" +
            "            {" +
            "              \"name\": \"_time\"," +
            "              \"type\": {" +
            "                \"type\": \"long\"," +
            "                \"logicalType\": \"nTimestamp\"" +
            "              }" +
            "            }," +
            "            {" +
            "              \"name\": \"samplePropKey\"," +
            "              \"type\": [" +
            "                \"null\"," +
            "                \"string\"" +
            "              ]" +
            "            }" +
            "          ]" +
            "        }" +
            "      }" +
            "    }" +
            "  ]" +
            "}";

    public static final String ALLTYPES_AVRO_SCHEMA = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"structure\"," +
            "    \"gdprDataCategory\": \"\",\n" +
            "    \"structureId\": \"ALLTYPES_NF\",\n" +
            "    \"tenant\": \"tenant-guid-1\",\n" +
            "    \"fields\": [{" +
            "        \"name\": \"messageId\"," +
            "        \"type\": \"string\"" +
            "    }, {" +
            "        \"name\": \"identifier\"," +
            "        \"type\": \"string\"" +
            "    }, {" +
            "        \"name\": \"tags\"," +
            "        \"type\": {" +
            "            \"type\": \"array\"," +
            "            \"items\": {" +
            "                \"type\": \"record\"," +
            "                \"name\": \"queryParams\"," +
            "                \"fields\": [{" +
            "                        \"name\": \"equipmentId\"," +
            "                        \"type\": \"string\"" +
            "                    }," +
            "                    {" +
            "                        \"name\": \"indicatorGroupId\"," +
            "                        \"type\": \"string\"" +
            "                    }, {" +
            "                        \"name\": \"modelId\"," +
            "                        \"type\": \"string\"" +
            "                    }, {" +
            "                        \"name\": \"templateId\"," +
            "                        \"type\": \"string\"" +
            "                    }" +
            "                ]" +
            "            }" +
            "        }" +
            "    }, {" +
            "        \"name\": \"measurements\"," +
            "        \"type\": {" +
            "            \"type\": \"array\"," +
            "            \"items\": {" +
            "                \"type\": \"record\"," +
            "                \"name\": \"timeseriesRecord\"," +
            "                \"fields\": [{" +
            "                        \"name\": \"_time\"," +
            "                        \"type\": {" +
            "                            \"type\": \"long\"," +
            "                            \"logicalType\": \"nTimestamp\"" +
            "                        }" +
            "                    }, {" +
            "                        \"name\": \"NumericFlexible\"," +
            "                        \"type\": [\"null\", \"float\"]" +
            "                    }," +
            "                    {" +
            "                        \"name\": \"Boolean\"," +
            "                        \"type\": [\"null\", \"boolean\"]" +
            "                    }, {" +
            "                        \"name\": \"String\"," +
            "                        \"type\": [\"null\", \"string\"]" +
            "                    }, {" +
            "                        \"name\": \"Date\"," +
            "                        \"type\": [\"null\", {" +
            "                            \"type\": \"long\"," +
            "                            \"logicalType\": \"timestamp-millis\"" +
            "                        }]" +
            "                    }, {" +
            "                        \"name\": \"Timestamp\"," +
            "                        \"type\": [\"null\", {" +
            "                            \"type\": \"long\"," +
            "                            \"logicalType\": \"timestamp-millis\"" +
            "                        }]" +
            "                    }," +
            "                    {" +
            "                        \"name\": \"DateTime\"," +
            "                        \"type\": [\"null\", {" +
            "                            \"type\": \"long\"," +
            "                            \"logicalType\": \"timestamp-millis\"" +
            "                        }]" +
            "                    }, {" +
            "                        \"name\": \"JSON\"," +
            "                        \"type\": [\"null\"," +
            "                            {" +
            "                                \"type\": \"string\"," +
            "                                \"logicalType\": \"nJson\"," +
            "                                \"max-length\": 65536" +
            "                            }" +
            "                        ]" +
            "                    }, {" +
            "                        \"name\": \"LargeString\"," +
            "                        \"type\": [\"null\", {" +
            "                            \"type\": \"string\"," +
            "                            \"logicalType\": \"nLargeString\"," +
            "                            \"max-length\": 524288" +
            "                        }]" +
            "                    }, {" +
            "                        \"name\": \"Int\"," +
            "                        \"type\": [\"null\", {" +
            "                            \"type\": \"int\"," +
            "                            \"logicalType\": \"int\"" +
            "                        }]" +
            "                    }, {" +
            "                        \"name\": \"Long\"," +
            "                        \"type\": [\"null\", {" +
            "                            \"type\": \"long\"," +
            "                            \"logicalType\": \"long\"" +
            "                        }]" +
            "                    }, {" +
            "                        \"name\": \"Double\"," +
            "                        \"type\": [\"null\", {" +
            "                            \"type\": \"double\"," +
            "                            \"logicalType\": \"double\"" +
            "                        }]" +
            "                    }, {" +
            "                        \"name\": \"Decimal\"," +
            "                        \"type\": [\"null\", {" +
            "                            \"type\": \"bytes\"," +
            "                            \"logicalType\": \"nDecimal\"," +
            "                            \"scale\": 10," +
            "                            \"precision\": 1" +
            "                        }]" +
            "                    }" +
            "                ]" +
            "            }" +
            "        }" +
            "    }]" +
            "}";
}
