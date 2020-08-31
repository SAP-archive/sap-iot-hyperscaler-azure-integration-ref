package com.sap.iot.azure.ref.ingestion.processing;

import com.sap.iot.azure.ref.integration.commons.mapping.MappingHelper;
import com.sap.iot.azure.ref.integration.commons.model.mapping.SensorMappingInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMapping;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.Tag;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessage;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasure;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasureKey;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.processed.ProcessedMessageContainer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeviceToProcessedMessageProcessorTest {

    @Mock
    private MappingHelper mappingHelperMock;

    private DeviceToProcessedMessageProcessor deviceToProcessedMessageProcessorSpy;

    private final String SAMPLE_SENSOR_ID = "sensorId";
    private final String SAMPLE_CAPABILITY_ID = "capId";
    private final String SAMPLE_SOURCE_ID = "sourceId";
    private final String SAMPLE_STRUCTURE_ID = "strucId";
    private final Instant SAMPLE_TIMESTAMP = Instant.now();
    private final String SAMPLE_PROPERTY_KEY = "propKey";


    private final int NUMBER_OF_GROUPS = 2;

    @Before
    public void setup() {
        createRawToProcessedMessageMapperWithMockMappingHelper();
    }

    private void createRawToProcessedMessageMapperWithMockMappingHelper() {
        // return mocked Mapping info
        DeviceToProcessedMessageProcessor deviceToProcessedMessageProcessor = new DeviceToProcessedMessageProcessor(mappingHelperMock);
        deviceToProcessedMessageProcessorSpy = Mockito.spy(deviceToProcessedMessageProcessor);
    }

    @Test
    public void testMapping() {
        AtomicInteger index = new AtomicInteger();
        getSampleMessages().entrySet()
                .forEach(rawMessageGrouping -> {
                    initMappingHelperMock();
                    Map.Entry<String, ProcessedMessageContainer> processedMessage = deviceToProcessedMessageProcessorSpy.process(rawMessageGrouping);

                    // deviceMapping is fetched with sensorId and capabilityId
                    verify(mappingHelperMock, times(1)).getSensorMapping(contains(SAMPLE_SENSOR_ID), contains(SAMPLE_CAPABILITY_ID));

                    List<ProcessedMessage> processedMeasures = processedMessage.getValue().getProcessedMessages();
                    String sourceId = SAMPLE_SOURCE_ID + index.get();

                    for (ProcessedMessage processedMessageEntry : processedMeasures) {

                        //output should include correct info
                        Assert.assertEquals(processedMessage.getKey(),
                                processedMessageEntry.getSourceId() + Constants.SEPARATOR + processedMessageEntry.getStructureId());
                        assertEquals(sourceId, processedMessageEntry.getSourceId());
                        assertEquals(SAMPLE_STRUCTURE_ID, processedMessageEntry.getStructureId());
                        assertEquals(SAMPLE_TIMESTAMP.toEpochMilli(), processedMessageEntry.getMeasures().get(0).get(Constants.TIMESTAMP_PROPERTY_KEY));
                    }

                    index.getAndIncrement();
                });
    }

    private Map<DeviceMeasureKey, List<DeviceMeasure>> getSampleMessages() {
        List<DeviceMeasure> messages = new ArrayList<>();
        Map<String, Object> properties = new HashMap<>();
        properties.put(SAMPLE_PROPERTY_KEY, "asd");

        IntStream.range(0, NUMBER_OF_GROUPS).forEach(i -> {
            messages.add(
                    DeviceMeasure.builder()
                            .sensorId(SAMPLE_SENSOR_ID + i)
                            .capabilityId(SAMPLE_CAPABILITY_ID + i)
                            .timestamp(SAMPLE_TIMESTAMP)
                            .properties(properties)
                            .build()
            );
        });

        return messages.stream().collect(Collectors.groupingBy(DeviceMeasure::getGroupingKey));
    }

    private void initMappingHelperMock() {
        reset(mappingHelperMock);
        IntStream.range(0, NUMBER_OF_GROUPS).forEach(i -> {
            Mockito.doReturn(getSampleMappingInfo(i)).when(mappingHelperMock).getSensorMapping(eq(SAMPLE_SENSOR_ID + i), eq(SAMPLE_CAPABILITY_ID + i));
        });
    }

    private SensorMappingInfo getSampleMappingInfo(int SourceIdPostFix) {
        return SensorMappingInfo.builder()
                .sourceId(SAMPLE_SOURCE_ID + SourceIdPostFix)
                .structureId(SAMPLE_STRUCTURE_ID)
                .tags(Collections.singletonList(Tag.builder().tagSemantic("sampleTagKey").tagValue("tagValue").build()))
                .propertyMappings(Collections.singletonList(PropertyMapping.builder().structurePropertyId(SAMPLE_PROPERTY_KEY).capabilityPropertyId(SAMPLE_PROPERTY_KEY).build()))
                .build();
    }
}