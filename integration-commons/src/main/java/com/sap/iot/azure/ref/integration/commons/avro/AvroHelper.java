package com.sap.iot.azure.ref.integration.commons.avro;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.integration.commons.adx.ADXConstants;
import com.sap.iot.azure.ref.integration.commons.avro.logicaltypes.RegisterService;
import com.microsoft.azure.eventhubs.impl.ClientConstants;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.AvroIngestionException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.*;

public class AvroHelper {

    private static GenericData gd = RegisterService.initializeCustomTypes();

    private static int EVENT_HUB_MSG_BODY_SIZE_LIMIT;

    static {
        // Basic tier is the default type with ~256 KB (supported message size) and tolerance of 512B for headers
        EVENT_HUB_MSG_BODY_SIZE_LIMIT = (EVENTHUB_SKU_BASIC_TIER_SIZE * 1024) - ClientConstants.MAX_EVENTHUB_AMQP_HEADER_SIZE_BYTES;

        if (EVENTHUB_SKU_TIER.equals(EVENTHUB_SKU_STANDARD_TIER)) {
            // ~1024 KB (supported message size in standard tier) with tolerance of 512B for headers
            EVENT_HUB_MSG_BODY_SIZE_LIMIT = (EVENTHUB_SKU_STANDARD_TIER_SIZE * 1024) - ClientConstants.MAX_EVENTHUB_AMQP_HEADER_SIZE_BYTES;
            InvocationContext.getContext().getLogger().log(Level.INFO, "Standard tier is configured for Event Hub");
        }
        else {
            InvocationContext.getContext().getLogger().log(Level.INFO, "Basic tier is configured for Event Hub");
        }
    }

    /**
     * Convert ProcessedMessage POJO to AVRO message.
     *
     * @param processedMessage, message which will be converted to AVRO message
     * @param schemaStr,        AVRO schema used for AVRO conversion
     * @return AVRO message
     * @throws AvroIngestionException exception in avro processing
     */
    public static byte[] serializeJsonToAvro(ProcessedMessage processedMessage, String schemaStr) throws AvroIngestionException {
        DatumWriter<GenericRecord> writer;
        DataFileWriter<GenericRecord> fileWriter = null;

        try {
            Schema schema = new Schema.Parser().parse(schemaStr);
            writer = gd.createDatumWriter(schema);

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            fileWriter = new DataFileWriter<>(writer);

            fileWriter.create(schema, byteArrayOutputStream);
            fileWriter.append(getGenericRecord(processedMessage, schema));
            fileWriter.flush();

            return byteArrayOutputStream.toByteArray();
        } catch (IOException | RuntimeException e) {
            throw new AvroIngestionException("Error in serializing processed message (json) to Avro", e,
                    IdentifierUtil.getIdentifier(CommonConstants.SOURCE_ID_PROPERTY_KEY, processedMessage.getSourceId(),
                            CommonConstants.STRUCTURE_ID_PROPERTY_KEY, processedMessage.getStructureId()));
        } finally {
            IOUtils.closeQuietly(fileWriter);
        }
    }

    /**
     * Convert the processed messages POJO to list of batched avro-messages each message limited by the allowed size limit on EventHub
     * Note - if a single {@link ProcessedMessage} is more than allowed size limit of Event Hub, this method cannot handle this.
     *
     * @param processedMessages, message which will be converted to AVRO message
     * @param schemaStr,        AVRO schema used for AVRO conversion
     * @return AVRO message
     * @throws AvroIngestionException exception in avro processing
     */
    public static List<byte[]> serializeJsonToAvro(List<ProcessedMessage> processedMessages, String schemaStr) throws AvroRuntimeException {

        List<byte[]> avroMessages = new LinkedList<>();
        DataFileWriter<GenericRecord> fileWriter = null;
        DatumWriter<GenericRecord> datumWriter;

        try {

            // setup the file writer for the first batch of avro message
            Schema schema = new Schema.Parser().parse(schemaStr);
            datumWriter = gd.createDatumWriter(schema);

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            fileWriter = new DataFileWriter<>(datumWriter);
            fileWriter.create(schema, byteArrayOutputStream);
            fileWriter.flush();

            int currentAvroMessageSize = 0;
            int lastValidSize;
            for (int i = 0; i < processedMessages.size(); ) {

                fileWriter.append(getGenericRecord(processedMessages.get(i), schema));

                /*
                will always flush to buffer (ByteArrayOutputStream) on each record so that size() returns the most recent size.
                From a performance point of view, since the file writer is backed by in memory byte-array output stream and not any i/o (actual file), so
                invoking flush on every append is fine; this method serializeJsonToAvro takes almost same time in both cases (flushing on every message vs
                flusing once all message are written) based on tests
                 */
                fileWriter.flush();

                lastValidSize = currentAvroMessageSize; // size before exceeding the eventhub message size limit
                currentAvroMessageSize = byteArrayOutputStream.size();
                if (currentAvroMessageSize < EVENT_HUB_MSG_BODY_SIZE_LIMIT) {
                    // continue with next messages
                    i++;

                } else {

                    // add the batched message formed until now to output
                    byte[] avroMessage = Arrays.copyOf(byteArrayOutputStream.toByteArray(), lastValidSize);
                    avroMessages.add(avroMessage);

                    InvocationContext.getLogger().fine("Avro Message size: " + avroMessage.length);
                    IOUtils.closeQuietly(fileWriter);

                    // prepare for next avro message
                    datumWriter = gd.createDatumWriter(schema);
                    byteArrayOutputStream = new ByteArrayOutputStream();
                    fileWriter = new DataFileWriter<>(datumWriter);
                    fileWriter.create(schema, byteArrayOutputStream);
                    fileWriter.flush();

                    // reset the size counters
                    currentAvroMessageSize = 0;
                }
            }

            // add the last batch to the output
            avroMessages.add(byteArrayOutputStream.toByteArray());

            return avroMessages;
        } catch (IOException | RuntimeException e) {

            ObjectNode exceptionId = IdentifierUtil.empty();
            if (processedMessages.size() > 0) {
                exceptionId = IdentifierUtil.getIdentifier(CommonConstants.SOURCE_ID_PROPERTY_KEY, processedMessages.get(0).getSourceId(),
                        CommonConstants.STRUCTURE_ID_PROPERTY_KEY, processedMessages.get(0).getStructureId());
            }

            throw new AvroIngestionException("Error in serializing processed message (json) to Avro", e, exceptionId);
        } finally {
            IOUtils.closeQuietly(fileWriter);
        }
    }

    private static GenericRecord getGenericRecord(ProcessedMessage processedMessage, Schema schema) throws AvroRuntimeException {

        GenericRecord datum = new GenericData.Record(schema);
        datum.put(AvroConstants.AVRO_DATUM_KEY_MESSAGE_ID, "" + processedMessage.getSourceId() + "/" + processedMessage.getStructureId() + "/" + new Date().getTime());
        datum.put(AvroConstants.AVRO_DATUM_KEY_IDENTIFIER, processedMessage.getSourceId());
        datum.put(AvroConstants.AVRO_DATUM_KEY_STRUCTURE_ID, processedMessage.getStructureId());
        datum.put(AvroConstants.AVRO_DATUM_KEY_TENANT, processedMessage.getTenantId());
        Schema measuresSchema = datum.getSchema().getField(AvroConstants.AVRO_DATUM_KEY_MEASUREMENTS).schema().getElementType();
        Schema tagSchema = datum.getSchema().getField(AvroConstants.AVRO_DATUM_KEY_TAGS).schema().getElementType();

        List<GenericRecord> tagsList = new ArrayList<>();
        List<GenericRecord> measuresList = new ArrayList<>();

        Map<String, String> tag = processedMessage.getTags();
        GenericRecord tagRecord = new GenericData.Record(tagSchema);
        for (Map.Entry<String, String> entry : tag.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            addMeasuresMatchingSchemaType(tagRecord, tagSchema.getField(key).schema(), key, value);
        }
        tagsList.add(tagRecord);
        datum.put(AvroConstants.AVRO_DATUM_KEY_TAGS, tagsList);

        for (Map<String, Object> measure : processedMessage.getMeasures()) {
            GenericRecord measureRecord = new GenericData.Record(measuresSchema);
            for (Map.Entry<String, Object> entry : measure.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                measureRecord.put(key, value);
                addMeasuresMatchingSchemaType(measureRecord, measuresSchema.getField(key).schema(), key, value);
            }
            measuresList.add(measureRecord);
            datum.put(AvroConstants.AVRO_DATUM_KEY_MEASUREMENTS, measuresList);
        }

        return datum;
    }

    private static void addMeasuresMatchingSchemaType(GenericRecord genericRecord, Schema fieldSchema, String key, Object value) throws AvroRuntimeException {
        boolean isNullable = false;
        if (fieldSchema.getType().getName().equals("union")) {
            if (fieldSchema.getTypes().size() > 0 && fieldSchema.getTypes().get(0).getType().getName().equals("null"))
                isNullable = true;
            fieldSchema = fieldSchema.getTypes().get(1);
        }
        if (isNullable && value == null) {
            genericRecord.put(key, null);
            return;
        }

        String expectedJavaClass = getJavaClassName(fieldSchema);
        switch (expectedJavaClass) {
            case "String":
                genericRecord.put(key, value.toString());
                break;
            case "Integer":
                genericRecord.put(key, Integer.parseInt(value.toString()));
                break;
            case "Long":
                genericRecord.put(key, Long.parseLong(value.toString()));
                break;
            case "Float":
                genericRecord.put(key, Float.parseFloat(value.toString()));
                break;
            case "Double":
                genericRecord.put(key, Double.parseDouble(value.toString()));
                break;
            case "Boolean":
                genericRecord.put(key, Boolean.parseBoolean(value.toString()));
                break;
            case "Instant":
                if (value.getClass() == Long.class) {
                    genericRecord.put(key, value);
                } else if (value.getClass() == String.class) {
                    genericRecord.put(key, Instant.parse(value.toString()).toEpochMilli());
                } else {
                    throw new AvroRuntimeException("Unsupported instant format at: " + key + " - " + value);
                }
                break;
            case "BigDecimal":
                genericRecord.put(key, new BigDecimal(value.toString()));
                break;
            default:
                throw new AvroRuntimeException("Unexpected type at: " + key + " - " + value);
        }
    }

    @VisibleForTesting
    static String getJavaClassName(Schema fieldSchema) {
        String type = fieldSchema.getLogicalType() == null ? fieldSchema.getType().getName() : fieldSchema.getLogicalType().getName();

        switch (type) {
            case "union":
                List<Schema> types = fieldSchema.getTypes();
                for (Schema t : types) {
                    String typeName = t.getType().getName();
                    if (typeName.equalsIgnoreCase("null"))
                        continue;
                    return getJavaClassName(t);
                }
            case "boolean":
                return "Boolean";
            case "int":
                return "Integer";
            case "long":
                return "Long";
            case "float":
                return "Float";
            case "double":
                return "Double";
            case "decimal":
            case "nDecimal":
                return "BigDecimal";
            case "bytes":
            case "nByte":
                return "byte[]";
            case "timestamp-millis":
            case "nTimestamp":
                return "Instant";
            case "string":
            case "nString":
            case "nJson":
            case "nLargeString":
                return "String";
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    /**
     * Get ADX column information for a given schema string.
     * The column information consists of certain default columns, such as source ID and enqueued time, as well as columns which are added depending on the
     * AVRO schemas measure and tag fields.
     *
     * @param schemaString, column information will be extracted from schema
     * @param structureId   structure id
     * @return {@link Map<String, String>} Map containing column name and types
     * @throws AvroIngestionException any error in avro processing
     */
    public static Map<String, String> getColumnInfo(String structureId, String schemaString) throws AvroIngestionException {
        try {
            Schema schema = new Schema.Parser().parse(schemaString);
            LinkedHashMap<String, String> columnInfo = new LinkedHashMap<>();

            columnInfo.put(CommonConstants.SOURCE_ID_PROPERTY_KEY, ADXConstants.ADX_DATATYPE_STRING);
            columnInfo.put(CommonConstants.ENQUEUED_TIME_PROPERTY_KEY, ADXConstants.ADX_DATATYPE_DATETIME);
            columnInfo.put(CommonConstants.DELETED_PROPERTY_KEY, ADXConstants.ADX_DATATYPE_BOOLEAN);
            List<Schema.Field> measureFields = schema.getField(AvroConstants.AVRO_DATUM_KEY_MEASUREMENTS).schema().getElementType().getFields();
            List<Schema.Field> tagFields = schema.getField(AvroConstants.AVRO_DATUM_KEY_TAGS).schema().getElementType().getFields();

            addFieldInfo(measureFields, columnInfo);
            addFieldInfo(tagFields, columnInfo);

            return columnInfo;
        } catch (AvroRuntimeException ex) {
            throw new AvroIngestionException("Error in fetching column types from Avro Scheam", ex,
                    IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId));
        }
    }

    private static LinkedHashMap<String, String> addFieldInfo(List<Schema.Field> measureFields, LinkedHashMap<String, String> columnInfo) {
        measureFields.forEach(measure -> {
            Schema measureSchema = measure.schema();
            String name = measure.name();
            if (measureSchema.getType().getName().equals(AvroConstants.AVRO_SCHEMA_UNION)) {
                measureSchema = measureSchema.getTypes().get(1);
            }

            columnInfo.put(name, getADXDataType(measureSchema));
        });

        return columnInfo;
    }

    private static String getADXDataType(Schema fieldSchema) {
        String type = fieldSchema.getLogicalType() == null ? fieldSchema.getType().getName() : fieldSchema.getLogicalType().getName();

        switch (type) {
            case AvroConstants.AVRO_SCHEMA_UNION:
                List<Schema> types = fieldSchema.getTypes();
                for (Schema t : types) {
                    String typeName = t.getType().getName();
                    if (typeName.equalsIgnoreCase(AvroConstants.AVRO_SCHEMA_NULL))
                        continue;
                    return getADXDataType(t);
                }
            case "boolean":
                return ADXConstants.ADX_DATATYPE_BOOLEAN;
            case "int":
                return "int";
            case "long":
                return "long";
            case "float":
            case "double":
                return "real";
            case "decimal":
            case "nDecimal":
                return "decimal";
            case "nTimestamp":
            case "timestamp-millis":
                return ADXConstants.ADX_DATATYPE_DATETIME;
            case "string":
            case "nString":
            case "nLargeString":
                return ADXConstants.ADX_DATATYPE_STRING;
            case "nJson":
                return ADXConstants.ADX_DATATYPE_DYNAMIC;
            default:
                throw new AvroIngestionException(String.format("Unsupported Datatype: %s.", type), IdentifierUtil.empty());
        }
    }
}
