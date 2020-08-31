package com.sap.iot.azure.ref.integration.commons.avro;

import com.microsoft.azure.eventhubs.impl.ClientConstants;
import com.sap.iot.azure.ref.integration.commons.avro.logicaltypes.RegisterService;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.AvroIngestionException;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@Slf4j
public class AvroHelperTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setUp() {
        RegisterService.initializeCustomTypes();
        InvocationContextTestUtil.initInvocationContext();
        environmentVariables.set("EVENTHUB_SKU_NAME", "eventhub-sku-tier");
    }

    @Test
    public void testAvroSerialization() {

        ProcessedMessage pm = getProcessedMessages(1).get(0);
        byte[] avro = AvroHelper.serializeJsonToAvro(pm, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_3);

        Map<String, Object> modifiedExpectedMeasures = pm.getMeasures().get(0);

        // converting all timestamp based fields to epoch-milli (long) base type in Avro
        modifiedExpectedMeasures.put("_time", Instant.parse(modifiedExpectedMeasures.get("_time").toString()).toEpochMilli());
        modifiedExpectedMeasures.put("Date", Instant.parse(modifiedExpectedMeasures.get("Date").toString()).toEpochMilli());
        modifiedExpectedMeasures.put("Timestamp", Instant.parse(modifiedExpectedMeasures.get("Timestamp").toString()).toEpochMilli());
        modifiedExpectedMeasures.put("DateTime", Instant.parse(modifiedExpectedMeasures.get("DateTime").toString()).toEpochMilli());

        ProcessedMessage deserializedMessage = deserializeAvro(avro, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_3).get(0);
        assertEquals(pm.toString(), deserializedMessage.toString());
    }

    @Test
    public void testInvalidSchema() {
        Map<String, String> tags = new HashMap<>();
        tags.put("indicatorGroupId", "string");

        Map<String, Object> measures = new HashMap<>();
        measures.put("_time", "2019-04-09T22:58:28.805Z");
        List<Map<String, Object>> measuresList = new ArrayList<>();
        measuresList.add(measures);

        ProcessedMessage pm = ProcessedMessage.builder()
                .sourceId("SourceId")
                .structureId("structureId")
                .tenantId("tenantId")
                .tags(tags)
                .measures(measuresList)
                .build();

        expectedException.expect(AvroIngestionException.class);
        AvroHelper.serializeJsonToAvro(pm, TestAVROSchemaConstants.SAMPLE_INVALID_AVRO_SCHEMA);
    }

    @Test
    public void testInvalidTimestamp() {
        Map<String, String> tags = new HashMap<>();
        tags.put("indicatorGroupId", "string");

        Map<String, Object> measures = new HashMap<>();
        measures.put("_time", 120);
        List<Map<String, Object>> measuresList = new ArrayList<>();
        measuresList.add(measures);

        ProcessedMessage pm = ProcessedMessage.builder()
                .sourceId("SourceId")
                .structureId("structureId")
                .tenantId("tenantId")
                .tags(tags)
                .measures(measuresList)
                .build();

        expectedException.expect(AvroIngestionException.class);
        AvroHelper.serializeJsonToAvro(pm, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_3);
    }

    @Test
    public void testInvalidDataType() {
        Map<String, String> tags = new HashMap<>();
        tags.put("sampleTagKey", "string");

        Map<String, Object> measures = new HashMap<>();
        measures.put("_time", "2019-04-09T22:58:28.805Z");
        List<Map<String, Object>> measuresList = new ArrayList<>();
        measuresList.add(measures);

        ProcessedMessage pm = ProcessedMessage.builder()
                .sourceId("SourceId")
                .structureId("structureId")
                .tenantId("tenantId")
                .tags(tags)
                .measures(measuresList)
                .build();

        expectedException.expect(AvroIngestionException.class);
        AvroHelper.serializeJsonToAvro(pm, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_INVALID_TYPE);
    }

    @Test
    public void getJavaClassName() {
        assertEquals("Boolean", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("boolean")));
        assertEquals("Integer", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("int")));
        assertEquals("Long", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("long")));
        assertEquals("Float", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("float")));
        assertEquals("Double", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("double")));
        assertEquals("BigDecimal", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("decimal")));
        assertEquals("BigDecimal", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("nDecimal")));
        assertEquals("byte[]", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("bytes")));
        assertEquals("byte[]", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("nByte")));
        assertEquals("Instant", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("timestamp-millis")));
        assertEquals("Instant", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("nTimestamp")));
        assertEquals("String", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("string")));
        assertEquals("String", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("nString")));
        assertEquals("String", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("nJson")));
        assertEquals("String", AvroHelper.getJavaClassName(getFieldSchemaWithLogicalType("nLargeString")));
    }

    @Test
    public void testAvroSerializationWithBatchingLimit() {

        int numberOfMessages = 10_000;

        List<ProcessedMessage> processedMessages = getProcessedMessages(numberOfMessages); // will the list of processed messages into three avro messages
        List<byte[]> avroMessages = AvroHelper.serializeJsonToAvro(processedMessages, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_3);

        assertTrue(avroMessages.size() > 1);

        int actualMessageCount = 0;
        // ensure that size of each message is less than allowed limit of 1MB
        for (byte[] avroMessage : avroMessages) {
            assertTrue(avroMessage.length < (CommonConstants.EVENTHUB_SKU_BASIC_TIER_SIZE * 1024) - ClientConstants.MAX_EVENTHUB_AMQP_HEADER_SIZE_BYTES);

            List<ProcessedMessage> processedMessagesActual = deserializeAvro(avroMessage, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_3);
            actualMessageCount += processedMessagesActual.size();
        }

        // ensure that batching, all messages are processed
        assertEquals(numberOfMessages, actualMessageCount);
    }

    @NotNull
    private List<ProcessedMessage> getProcessedMessages(int numberOfMessages) {
        List<ProcessedMessage> processedMessages = new LinkedList<>();

        for (int i = 0; i < numberOfMessages; i++) {
            Map<String, String> tags = new HashMap<>();
            tags.put("equipmentId", "stringE");
            tags.put("templateId", "stringT");
            tags.put("modelId", "stringM");
            tags.put("indicatorGroupId", "stringI");

            Map<String, Object> measures = new HashMap<>();
            measures.put("_time", Instant.now().toString());
            measures.put("NumericFlexible", i + 1.1f);
            measures.put("Boolean", true);
            measures.put("String", "Some String" + i);
            measures.put("Date", "2019-04-09T22:58:28.805Z");
            measures.put("Timestamp", "2019-04-09T22:58:28.805Z");
            measures.put("DateTime", Instant.now().toString());
            measures.put("JSON", "{}");
            measures.put("Int", 123 + i);
            measures.put("Long", i + 123L);
            measures.put("Double", i + 123.4);
            measures.put("LargeString", "largeString" + i);
            measures.put("Decimal", new BigDecimal(1.1).setScale(2, RoundingMode.CEILING));

            ProcessedMessage pm = ProcessedMessage.builder()
                    .sourceId("SourceId")
                    .structureId("structureId")
                    .tenantId("tenantId")
                    .tags(tags)
                    .measures(Collections.singletonList(measures))
                    .build();

            processedMessages.add(pm);
        }
        return processedMessages;
    }

    private static Schema getFieldSchemaWithLogicalType(String logicalTypeName) {
        Schema schema = mock(Schema.class);
        LogicalType logicalType = mock(LogicalType.class);
        doReturn(logicalTypeName).when(logicalType).getName();
        doReturn(logicalType).when(schema).getLogicalType();

        return schema;
    }

    public static List<ProcessedMessage> deserializeAvro(byte[] avro, String schemaStr) {
        Schema schema = new Schema.Parser().parse(schemaStr);
        GenericData gd = RegisterService.initializeCustomTypes();
        ProcessedMessage pm = null;

        List<ProcessedMessage> processedMessages = new LinkedList<>();

        DatumReader datumReader = gd.createDatumReader(schema);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new SeekableByteArrayInput(avro), datumReader)) {
            GenericRecord genericRecord = null;

            while (dataFileReader.hasNext()) { // each avro message can have multiple avro records, in this testcase, we have one record
                genericRecord = dataFileReader.next(genericRecord);
                log.debug("Deserialized message: {}", genericRecord);

                Map<String, String> tags = new HashMap<>();
                List<Map<String, Object>> measuresList = new ArrayList<>();
                GenericData.Array tagsRecord = (GenericData.Array) genericRecord.get("tags");
                GenericData.Array measurementsRecord = (GenericData.Array) genericRecord.get("measurements");

                if (tagsRecord.size() > 0) {
                    GenericRecord gRec = (GenericRecord) tagsRecord.get(0);
                    List<Schema.Field> fields = gRec.getSchema().getFields();
                    for (int i = 0; i < fields.size(); i++) {
                        Object value = gRec.get(i);
                        tags.put(fields.get(i).name(), String.valueOf(value));
                    }
                }

                for (Object obj : measurementsRecord) {
                    Map<String, Object> measuresMap = new HashMap<>();
                    GenericRecord gRec = (GenericRecord) obj;
                    List<Schema.Field> fields = gRec.getSchema().getFields();
                    for (int i = 0; i < fields.size(); i++) {
                        Object value = gRec.get(i);
                        measuresMap.put(fields.get(i).name(), value);
                    }
                    measuresList.add(measuresMap);
                }

                pm = ProcessedMessage.builder()
                        .sourceId(String.valueOf(genericRecord.get("identifier")))
                        .structureId(String.valueOf(genericRecord.get("structureId")))
                        .tenantId(String.valueOf(genericRecord.get("tenant")))
                        .tags(tags)
                        .measures(measuresList)
                        .build();

                processedMessages.add(pm);
            }

        } catch (IOException ex) {
            log.error("Deserializing Avro message failed");
        }

        return processedMessages;
    }

}