package com.sap.iot.azure.ref.ingestion.output;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.microsoft.azure.eventhubs.BatchOptions;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.avro.AvroConstants;
import com.sap.iot.azure.ref.integration.commons.avro.logicaltypes.RegisterService;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.eventhub.BaseEventHubProcessorTest;
import com.sap.iot.azure.ref.integration.commons.util.CompletableFutures;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ProcessedTimeSeriesEventHubProcessorTest {
    private static final String CONNECTION_STRING = "testConnStr";

    @Mock
    EventHubClient ehclientMock;

    @Captor
    private ArgumentCaptor<EventDataBatch> eventDataCaptor;

    @ClassRule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables()
            .set(Constants.PROCESSED_TIME_SERIES_CONNECTION_STRING_PROP, CONNECTION_STRING);

    private ProcessedTimeSeriesEventHubProcessor processedTimeSeriesEventHubProcessor;

    @Before
    public void setup() throws EventHubException {
        processedTimeSeriesEventHubProcessor = new ProcessedTimeSeriesEventHubProcessor(CompletableFuture.completedFuture(ehclientMock));

        when(ehclientMock.createBatch(any(BatchOptions.class))).thenReturn(new BaseEventHubProcessorTest.SimpleEventBatch());
        when(ehclientMock.send(any(EventDataBatch.class))).thenReturn(CompletableFutures.voidCompletedFuture());
    }

    @BeforeClass
    public static void setupClass() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Test
    public void testProcess() throws Exception {
        //If I call process with a list of processed messages, they should be converted and sent as event data.
        processedTimeSeriesEventHubProcessor.apply(Maps.immutableEntry("sourceId", OutputTestUtil.createProcessedMessages()));

        verify(ehclientMock, times(1)).send(eventDataCaptor.capture());
        JsonNode processedMessage = decode(((BaseEventHubProcessorTest.SimpleEventBatch) eventDataCaptor.getValue()).getBytes(0));
        assertEquals(OutputTestUtil.SOURCE_ID, processedMessage.get(AvroConstants.AVRO_DATUM_KEY_IDENTIFIER).textValue());
        assertEquals(OutputTestUtil.SAMPLE_TAG_VAL, processedMessage.get(AvroConstants.AVRO_DATUM_KEY_TAGS).get(0).get(OutputTestUtil.SAMPLE_TAG_KEY).asText());
        assertEquals(OutputTestUtil.SAMPLE_PROPERTY_VAL, processedMessage.get(AvroConstants.AVRO_DATUM_KEY_MEASUREMENTS).get(0).get(OutputTestUtil.SAMPLE_PROPERTY_KEY).asText());
    }

    private JsonNode decode ( byte[] avro ) throws Exception {
        GenericData genericData = RegisterService.initializeCustomTypes();
        DatumReader<GenericRecord> readerWithoutSchema = new GenericDatumReader<>();
        GenericRecord genericRecord = null;
        InputStream is = new ByteArrayInputStream(avro);
        DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(is, readerWithoutSchema);
        if (dataFileStream.hasNext()) {
            genericRecord = dataFileStream.next(genericRecord);
        }
        dataFileStream.close();
        if (genericRecord != null) {

            Schema schema = genericRecord.getSchema();

            SeekableByteArrayInput sin = new SeekableByteArrayInput(avro);
            @SuppressWarnings("unchecked")
            DatumReader<GenericRecord> readerWithSchema = genericData.createDatumReader(schema);
            genericRecord = null;
            DataFileReader<GenericRecord> in = new DataFileReader<>(sin, readerWithSchema);
            while(in.hasNext()) {
                genericRecord = in.next(genericRecord);
            }
            in.close();
        }
        return new ObjectMapper().readTree(genericRecord.toString());
    }
}