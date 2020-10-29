package com.sap.iot.azure.ref.ingestion.output;

import com.sap.iot.azure.ref.ingestion.avro.TestAVROSchemaConstants;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OutputTestUtil {

    static final String SOURCE_ID = "testSourceId";
    static final String STRUCTURE_ID = "testStructureId";
    static final String TENANT_ID = "testTenant";
    static final long TIMESTAMP = 1585774916094L;
    static final String SAMPLE_PROPERTY_KEY = "samplePropKey";
    static final String SAMPLE_PROPERTY_VAL = "samplePropValue";
    static final String SAMPLE_TAG_KEY = "sampleTagKey";
    static final String SAMPLE_TAG_VAL = "sampleTagVal";
    static final String AVRO_SCHEMA_MEASUREMENTS_FIELD_NAME = "measurements";

    static ProcessedMessageContainer createProcessedMessages() {

        ProcessedMessage processedMessage = ProcessedMessage.builder()
                .sourceId(SOURCE_ID)
                .measures(Collections.singletonList(createMeasure()))
                .tags(createTags())
                .build();

        return new ProcessedMessageContainer(TestAVROSchemaConstants.SIMPLE_AVRO_SCHEMA, STRUCTURE_ID, Collections.singletonList(processedMessage));
    }

    static Map<String, Object> createMeasure() {
        Map<String, Object> measure = new HashMap<>();
        measure.put(CommonConstants.TIMESTAMP_PROPERTY_KEY, TIMESTAMP);
        measure.put(SAMPLE_PROPERTY_KEY, SAMPLE_PROPERTY_VAL);

        return measure;
    }

    static Map<String, String> createTags() {
        Map<String, String> tags = new HashMap<>();
        tags.put(SAMPLE_TAG_KEY, SAMPLE_TAG_VAL);

        return tags;
    }
}
