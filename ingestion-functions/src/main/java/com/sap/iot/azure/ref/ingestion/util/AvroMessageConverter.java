package com.sap.iot.azure.ref.ingestion.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.integration.commons.avro.logicaltypes.RegisterService;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.exception.AvroIngestionException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.MappingLookupException;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingHelper;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.STRUCTURE_ID_PROPERTY_KEY;

public class AvroMessageConverter {

    private final MappingHelper mappingHelper;

    private static GenericData genericData = RegisterService.initializeCustomTypes();

    public AvroMessageConverter() {
        this(new MappingHelper());
    }

    @VisibleForTesting
    AvroMessageConverter(MappingHelper mappingHelper) {
        this.mappingHelper = mappingHelper;
    }

    /**
     * Deserializes avro message for a given structureId.
     * The schema is extracted and verified from the message. The genericRecord for measurements
     * is also converted to String type, if it is of Instant type during the process of deserialization.
     *
     * @param structureId, required for fetching schema information from the {@link MappingHelper},
     * @param avro, required avro message to be deserialized from byte[] to List<JsonNode>
     * @return list<JsonNode> of {@link List<JsonNode> messages} after deserialization of avroMessage
     */
    public List<JsonNode> deserializeAvroMessage(String structureId, byte[] avro) throws MappingLookupException, ADXClientException, AvroIngestionException {

        try {
            // INFO Call mapping helper to check ADX table existence
            mappingHelper.getSchemaInfo(structureId);

            return genericMessageAvroDecoder(avro);
        } catch ( IOException | RuntimeException e) {
            throw new AvroIngestionException("Avro Message cannot be de-serialized", e, IdentifierUtil.getIdentifier(STRUCTURE_ID_PROPERTY_KEY, structureId));
        }
    }

    private List<JsonNode> genericMessageAvroDecoder(byte[] avro) throws IOException {
        List<JsonNode> deserializedMessages = new LinkedList<>();
        JsonNode genericJSON;
        DatumReader<GenericRecord> readerWithoutSchema = new GenericDatumReader<>();
        GenericRecord genericRecord = null;
        try (InputStream is = new ByteArrayInputStream(avro);
                DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(is, readerWithoutSchema)) {
            if (dataFileStream.hasNext()) {
                genericRecord = dataFileStream.next(genericRecord);
            }
            if (genericRecord != null) {
                Schema schema = genericRecord.getSchema();
                SeekableByteArrayInput sin = new SeekableByteArrayInput(avro);
                @SuppressWarnings("unchecked")
                DatumReader<GenericRecord> readerWithSchema = genericData.createDatumReader(schema);
                genericRecord = null;
                try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(sin, readerWithSchema)) {
                    while (dataFileReader.hasNext()) {
                        genericRecord = dataFileReader.next(genericRecord);
                        genericJSON = new ObjectMapper().readTree(genericRecord.toString());
                        deserializedMessages.add(genericJSON);
                    }
                }
            } else {
                throw new IOException("Avro GenericRecord is empty.");
            }
        }
        return deserializedMessages;
    }
}
