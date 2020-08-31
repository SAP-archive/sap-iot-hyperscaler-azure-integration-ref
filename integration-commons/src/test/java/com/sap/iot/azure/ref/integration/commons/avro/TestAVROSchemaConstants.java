package com.sap.iot.azure.ref.integration.commons.avro;

public class TestAVROSchemaConstants {
    public static final String SAMPLE_AVRO_SCHEMA = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"schema\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\": \"tags\"," +
            "            \"type\": {" +
            "                \"type\": \"array\"," +
            "                \"items\": {" +
            "                    \"type\": \"record\"," +
            "                    \"name\": \"queryParams\"," +
            "                    \"fields\": [" +
            "                    ]" +
            "                }" +
            "            }" +
            "        }," +
            "        {" +
            "            \"name\": \"measurements\"," +
            "            \"type\": {" +
            "                \"type\": \"array\"," +
            "                \"items\": {" +
            "                    \"type\": \"record\"," +
            "                    \"name\": \"timeseriesRecord\"," +
            "                    \"fields\": [" +
            "                    ]" +
            "                }" +
            "            }" +
            "        }" +
            "    ]" +
            "}";

    public static final String SAMPLE_COMPLEX_AVRO_SCHEMA = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"schema\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\": \"tags\"," +
            "            \"type\": {" +
            "                \"type\": \"array\"," +
            "                \"items\": {" +
            "                    \"type\": \"record\"," +
            "                    \"name\": \"queryParams\"," +
            "                    \"fields\": [" +
            "                        {" +
            "                            \"name\": \"tag1\"," +
            "                            \"type\": [" +
            "                                \"null\"," +
            "                                \"string\"" +
            "                            ]" +
            "                        }," +
            "                        {" +
            "                            \"name\": \"tag2\"," +
            "                            \"type\": [" +
            "                                \"null\"," +
            "                                \"string\"" +
            "                            ]" +
            "                        }," +
            "                        {" +
            "                            \"name\": \"tag3\"," +
            "                            \"type\": [" +
            "                                \"null\"," +
            "                                \"string\"" +
            "                            ]" +
            "                        }," +
            "                        {" +
            "                            \"name\": \"tag4\"," +
            "                            \"type\": [" +
            "                                \"null\"," +
            "                                \"string\"" +
            "                            ]" +
            "                        }" +
            "                    ]" +
            "                }" +
            "            }" +
            "        }," +
            "        {" +
            "            \"name\": \"measurements\"," +
            "            \"type\": {" +
            "                \"type\": \"array\"," +
            "                \"items\": {" +
            "                    \"type\": \"record\"," +
            "                    \"name\": \"timeseriesRecord\"," +
            "                    \"fields\": [" +
            "                        {" +
            "                            \"name\": \"_time\"," +
            "                            \"type\": {" +
            "                                \"type\": \"long\"," +
            "                                \"logicalType\": \"nTimestamp\"" +
            "                            }" +
            "                        }," +
            "                        {" +
            "                            \"name\": \"decimalMeasure\"," +
            "                            \"type\": [" +
            "                                \"null\"," +
            "                                {" +
            "                                    \"type\": \"bytes\"," +
            "                                    \"logicalType\": \"nDecimal\"," +
            "                                    \"scale\": 10," +
            "                                    \"precision\": 0" +
            "                                }" +
            "                            ]" +
            "                        }," +
            "                        {" +
            "                            \"name\": \"booleanMeasure\"," +
            "                            \"type\": [" +
            "                                \"null\"," +
            "                                {" +
            "                                    \"type\": \"boolean\"," +
            "                                    \"logicalType\": \"boolean\"" +
            "                                }" +
            "                            ]" +
            "                        }," +
            "                        {" +
            "                            \"name\": \"intMeasure\"," +
            "                            \"type\": [" +
            "                                \"null\"," +
            "                                {" +
            "                                    \"type\": \"int\"," +
            "                                    \"logicalType\": \"int\"" +
            "                                }" +
            "                            ]" +
            "                        }," +
            "                        {" +
            "                            \"name\": \"longMeasure\"," +
            "                            \"type\": [" +
            "                                \"null\"," +
            "                                {" +
            "                                    \"type\": \"long\"," +
            "                                    \"logicalType\": \"long\"" +
            "                                }" +
            "                            ]" +
            "                        }," +
            "                        {" +
            "                            \"name\": \"floatMeasure\"," +
            "                            \"type\": [" +
            "                                \"null\"," +
            "                                {" +
            "                                    \"type\": \"float\"," +
            "                                    \"logicalType\": \"float\"" +
            "                                }" +
            "                            ]" +
            "                        }" +
            "                    ]" +
            "                }" +
            "            }" +
            "        }" +
            "    ]" +
            "}";

    public static final String SAMPLE_INVALID_AVRO_SCHEMA = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"schema\"," +
            "    \"fields\": [" +
            "        {" +
            "            \"name\": \"tags\"," +
            "            \"type\": {" +
            "                \"type\": \"array\"," +
            "                \"items\": {" +
            "                    \"type\": \"record\"," +
            "                    \"name\": \"queryParams\"," +
            "                    \"fields\": [" +
            "                    ]" +
            "                }" +
            "            }" +
            "        }," +
            "        {" +
            "            \"name\": \"measurements\"," +
            "            \"type\": {" +
            "                \"type\": \"array\"," +
            "                \"items\": {" +
            "                    \"type\": \"record\"," +
            "                    \"name\": \"timeseriesRecord\"," +
            "                    \"fields\": [" +
            "                        {" +
            "                            \"name\": \"invalidMeasurement\"," +
            "                            \"type\": {" +
            "                                \"type\": \"INVALID\"," +
            "                                \"logicalType\": \"nDecimal\"" +
            "                            }" +
            "                        }" +
            "                    ]" +
            "                }" +
            "            }" +
            "        }" +
            "    ]" +
            "}";

    public static final String SAMPLE_AVRO_SCHEMA_2 = "{" +
            "  \"type\": \"record\"," +
            "  \"name\": \"TEST_SCHEMA\"," +
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
            "      \"name\": \"structureId\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"tenant\"," +
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
            "              \"type\": \"string\"" +
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
            "              \"name\": \"mockKey\"," +
            "              \"type\": {" +
            "                \"type\": \"string\"" +
            "              }" +
            "            }" +
            "          ]" +
            "        }" +
            "      }" +
            "    }" +
            "  ]" +
            "}";

    public static final String SAMPLE_AVRO_SCHEMA_3 = "{" +
            "    \"type\": \"record\"," +
            "    \"name\": \"structure\"," +
            "    \"fields\": [{" +
            "        \"name\": \"messageId\"," +
            "        \"type\": \"string\"" +
            "    }, {" +
            "        \"name\": \"identifier\"," +
            "        \"type\": \"string\"" +
            "    }, {" +
            "        \"name\": \"structureId\"," +
            "        \"type\": \"string\"" +
            "    }, {" +
            "        \"name\": \"tenant\"," +
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
            "                            \"scale\": 2," +
            "                            \"precision\": 10" +
            "                        }]" +
            "                    }" +
            "                ]" +
            "            }" +
            "        }" +
            "    }]" +
            "}";

    public static final String SAMPLE_AVRO_SCHEMA_INVALID_TYPE = "{" +
            "  \"type\": \"record\"," +
            "  \"name\": \"TEST_SCHEMA\"," +
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
            "      \"name\": \"structureId\"," +
            "      \"type\": \"string\"" +
            "    }," +
            "    {" +
            "      \"name\": \"tenant\"," +
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
            "              \"type\": \"string\"" +
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
            "                \"logicalType\": \"timestamp-seconds\"" +
            "              }" +
            "            }," +
            "            {" +
            "              \"name\": \"mockKey\"," +
            "              \"type\": {" +
            "                \"type\": \"string\"" +
            "              }" +
            "            }" +
            "          ]" +
            "        }" +
            "      }" +
            "    }" +
            "  ]" +
            "}";
}
