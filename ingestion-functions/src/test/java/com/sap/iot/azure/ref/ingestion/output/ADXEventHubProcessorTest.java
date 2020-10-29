package com.sap.iot.azure.ref.ingestion.output;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.Maps;
import com.microsoft.azure.eventhubs.BatchOptions;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.adx.ADXConstants;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.eventhub.BaseEventHubProcessorTest;
import com.sap.iot.azure.ref.integration.commons.util.CompletableFutures;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ADXEventHubProcessorTest {
    private static final ObjectMapper om = new ObjectMapper();
    private static final ObjectReader reader = om.reader();
    private  static final String CONNECTION_STRING = "testConnStr";

    private ADXEventHubProcessor adxEventHubProcessor;

    @Mock
    EventHubClient ehClientMock;
    @Captor
    private ArgumentCaptor<EventDataBatch> eventDataCaptor;

    @ClassRule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables()
            .set(Constants.ADX_SOURCE_CONNECTION_STRING_PROP, CONNECTION_STRING);

    @Before
    public void setup() throws EventHubException {
        adxEventHubProcessor = new ADXEventHubProcessor(CompletableFuture.completedFuture(ehClientMock));

        when(ehClientMock.createBatch(any(BatchOptions.class))).thenReturn(new BaseEventHubProcessorTest.SimpleEventBatch());
        when(ehClientMock.send(any(EventDataBatch.class))).thenReturn(CompletableFutures.voidCompletedFuture());
    }


    @Test
    public void testProcess() throws IOException {
        //If I call process with a list of processed messages, they should be converted and sent as event data.
        adxEventHubProcessor.apply(Maps.immutableEntry("sourceId", OutputTestUtil.createProcessedMessages()));

        verify(ehClientMock, times(1)).send(eventDataCaptor.capture());
        BaseEventHubProcessorTest.SimpleEventBatch capturedEventBatch = (BaseEventHubProcessorTest.SimpleEventBatch) eventDataCaptor.getValue();

        JsonNode processedMessage = reader.readTree(new ByteArrayInputStream((capturedEventBatch.getBytes(0))));
        Map<String, Object> properties = capturedEventBatch.getProperties(0);

        String sourceId = processedMessage.get(CommonConstants.SOURCE_ID_PROPERTY_KEY).textValue();
        String timestamp = processedMessage.get(CommonConstants.TIMESTAMP_PROPERTY_KEY).textValue();
        String mockPropertyVal = processedMessage.get(OutputTestUtil.AVRO_SCHEMA_MEASUREMENTS_FIELD_NAME).get(OutputTestUtil.SAMPLE_PROPERTY_KEY).textValue();
        String sampleTagVal = processedMessage.get(OutputTestUtil.AVRO_SCHEMA_MEASUREMENTS_FIELD_NAME).get(OutputTestUtil.SAMPLE_TAG_KEY).textValue();

        // Check EventData Body
        assertEquals(OutputTestUtil.SOURCE_ID, sourceId);
        assertEquals(Instant.ofEpochMilli(OutputTestUtil.TIMESTAMP).toString(), timestamp);
        assertEquals(OutputTestUtil.SAMPLE_PROPERTY_VAL, mockPropertyVal);
        assertEquals(OutputTestUtil.SAMPLE_TAG_VAL, sampleTagVal);

        // Check EventData Properties
        assertEquals(ADXConstants.TABLE_PREFIX + OutputTestUtil.STRUCTURE_ID, properties.get(ADXConstants.TABLE_PROPERTY_KEY).toString());
        assertEquals(ADXConstants.MULTIJSON_FORMAT, properties.get(ADXConstants.FORMAT_PROPERTY_KEY).toString());
        assertEquals(ADXConstants.TABLE_PREFIX + OutputTestUtil.STRUCTURE_ID, properties.get(ADXConstants.MAPPING_PROPERTY_KEY).toString());
    }
}