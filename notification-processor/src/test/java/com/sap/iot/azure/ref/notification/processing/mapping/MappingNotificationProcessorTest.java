package com.sap.iot.azure.ref.notification.processing.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.cache.CacheKeyBuilder;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMapping;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMappingInfo;
import com.sap.iot.azure.ref.notification.processing.NotificationMessage;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import sun.nio.ch.IOUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MappingNotificationProcessorTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Mock
    private CacheRepository cacheRepository;
    private MappingNotificationProcessor mappingNotificationProcessor;
    private String sampleMappingId = "047a01e6-d168-4a4e-864f-1db696320fcd";
    private String sampleStructureId = "E10100304AEFE7A616005E02C64AM110";
    private String sampleVirtualCapId = "E10100304AEFE7A616005E02C64AM110";
    private List<PropertyMapping> propertyMappings = new ArrayList<>();
    private NotificationMessage notificationMessage;

    @Before
    public void setup() throws IOException {
        mappingNotificationProcessor = new MappingNotificationProcessor(cacheRepository);
        propertyMappings.clear();
    }

    @Test
    public void testCreate() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationCreateMessage.json"), "UTF-8");
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        mappingNotificationProcessor.handleCreate(notificationMessage);
        String structurePropertyId = "Bearing_Temperature_M110";
        String capabilityPropertyId = "Bearing_Temperature_M110";
        propertyMappings.add(addPropertyMapping(structurePropertyId, capabilityPropertyId));
        PropertyMappingInfo propertyMappingInfo = PropertyMappingInfo.builder().mappingId(sampleMappingId).structureId(sampleStructureId).virtualCapabilityId(sampleVirtualCapId).propertyMappings(propertyMappings).build();
        verify(cacheRepository, times(1)).set(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId, sampleStructureId, sampleVirtualCapId), propertyMappingInfo, PropertyMappingInfo.class);
    }

    @Test
    public void testDelete() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationDeleteMessage.json"), "UTF-8");
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        String cacheKey = "SAP_" + "MAPPING_" + notificationMessage.getChangeEntity();
        List<String> cacheKeys = new ArrayList<String>();
        cacheKeys.add(cacheKey);
        Mockito.when(cacheRepository.scanCacheKey(MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX
                + MappingServiceConstants.CACHE_MAPPING_KEY_PREFIX
                + notificationMessage.getChangeEntity()))
                .thenReturn(cacheKeys);
        mappingNotificationProcessor.handleDelete(notificationMessage);
        verify(cacheRepository, times(1)).scanCacheKey(cacheKey);
        verify(cacheRepository, times(1)).delete(cacheKey.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testUpdateAddMeasure() throws IOException {
        //create mapping
        String structureId="E10100304AEFE7A616005E02C64AM112";
        String virtualCapId="E10100304AEFE7A616005E02C64AM112";
        String structurePropertyId = "Bearing_Temperature_M110";
        String capabilityPropertyId = "Bearing_Temperature_M110";
        propertyMappings.add(addPropertyMapping(structurePropertyId, capabilityPropertyId));
        PropertyMappingInfo propertyMappingInfo = PropertyMappingInfo.builder().mappingId(sampleMappingId).structureId(structureId).virtualCapabilityId(virtualCapId).propertyMappings(propertyMappings).build();

        //update mapping with notification message
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationUpdateAddMeasureMessage.json"), "UTF-8");
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        mappingNotificationProcessor.handleUpdate(notificationMessage);

        verify(cacheRepository, times(1)).get(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId, structureId, virtualCapId),
                PropertyMappingInfo.class);
        verify(cacheRepository, times(1)).set(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId, structureId, virtualCapId),
                propertyMappingInfo, PropertyMappingInfo.class);
    }

    @Test
    public void testUpdateAddMeasureProperty() throws IOException {

        //create mapping
        String structurePropertyId = "Bearing_Temperature_M110";
        String capabilityPropertyId = "Bearing_Temperature_M110";
        propertyMappings.add(addPropertyMapping(structurePropertyId, capabilityPropertyId));
        PropertyMappingInfo propertyMappingInfo = PropertyMappingInfo.builder().mappingId(sampleMappingId).structureId(sampleStructureId).virtualCapabilityId(sampleVirtualCapId).propertyMappings(propertyMappings).build();

        //update mapping with notification message
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationUpdateAddMeasurePropertyMessage.json"), "UTF-8");
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        Mockito.when(cacheRepository.get(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId, sampleStructureId, sampleVirtualCapId),
                PropertyMappingInfo.class))
                .thenReturn(java.util.Optional.ofNullable(propertyMappingInfo));

        mappingNotificationProcessor.handleUpdate(notificationMessage);
        verify(cacheRepository, times(1)).get(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId, sampleStructureId, sampleVirtualCapId),
                PropertyMappingInfo.class);
        verify(cacheRepository, times(1)).set(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId, sampleStructureId, sampleVirtualCapId),
                propertyMappingInfo, PropertyMappingInfo.class);

        List<PropertyMapping> propertyMappingsUpdated = new ArrayList<>();
        propertyMappingsUpdated.add(addPropertyMapping(structurePropertyId, capabilityPropertyId));
        String structurePropertyId_new = "Bearing_Temperature_M110_new";
        String capabilityPropertyId_new = "Bearing_Temperature_M110_new";
        String structurePropertyId_new2 = "Bearing_Temperature_M110_new2";
        String capabilityPropertyId_new2 = "Bearing_Temperature_M110_new2";
        propertyMappingsUpdated.add(addPropertyMapping(structurePropertyId_new, capabilityPropertyId_new));
        propertyMappingsUpdated.add(addPropertyMapping(structurePropertyId_new2, capabilityPropertyId_new2));

        assertEquals(propertyMappingInfo.getPropertyMappings(),propertyMappingsUpdated);
    }

    @Test
    public void testUpdateDeleteMeasure() throws IOException {
        //create mapping
        String structureId="E10100304AEFE7A616005E02C64AM112";
        String virtualCapId="E10100304AEFE7A616005E02C64AM112";
        propertyMappings.add(addPropertyMapping(structureId, virtualCapId));
        PropertyMappingInfo propertyMappingInfo = PropertyMappingInfo.builder().mappingId(sampleMappingId).structureId(structureId).virtualCapabilityId
                (virtualCapId).propertyMappings(propertyMappings).build();

        //update mapping with notification message
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationUpdateDeleteMeasureMessage.json"), "UTF-8");
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        mappingNotificationProcessor.handleUpdate(notificationMessage);
        verify(cacheRepository, times(1)).delete(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId,structureId,virtualCapId));
    }

    @Test
    public void testUpdateDeleteMeasureProperty() throws IOException {
        //create mapping
        String structurePropertyId = "Bearing_Temperature_M110_new";
        String capabilityPropertyId = "Bearing_Temperature_M110_new";
        propertyMappings.add(addPropertyMapping(structurePropertyId, capabilityPropertyId));
        PropertyMappingInfo propertyMappingInfo = PropertyMappingInfo.builder().mappingId(sampleMappingId).structureId(sampleStructureId).virtualCapabilityId(sampleVirtualCapId).propertyMappings(propertyMappings).build();

        //update mapping with notification message
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationUpdateDeleteMeasurePropertyMessage.json"), "UTF-8");
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        Mockito.when(cacheRepository.get(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId, sampleStructureId, sampleVirtualCapId),
                PropertyMappingInfo.class))
                .thenReturn(java.util.Optional.ofNullable(propertyMappingInfo));
        mappingNotificationProcessor.handleUpdate(notificationMessage);
        verify(cacheRepository, times(1)).get(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId, sampleStructureId, sampleVirtualCapId),
                PropertyMappingInfo.class);
        verify(cacheRepository, times(1)).set(CacheKeyBuilder.constructPropertyMappingInfoKey(sampleMappingId, sampleStructureId, sampleVirtualCapId),
                propertyMappingInfo, PropertyMappingInfo.class);

        List<PropertyMapping> propertyMappingsUpdated = new ArrayList<>();
        propertyMappingsUpdated.add(addPropertyMapping(structurePropertyId, capabilityPropertyId));
        propertyMappingsUpdated.remove(addPropertyMapping(structurePropertyId, capabilityPropertyId));
        assertEquals(propertyMappingInfo.getPropertyMappings(),propertyMappingsUpdated);
    }

    private PropertyMapping addPropertyMapping(String structurePropertyId, String capabilityPropertyId) {
        PropertyMapping propertyMapping = PropertyMapping.builder().capabilityPropertyId(capabilityPropertyId).structurePropertyId(structurePropertyId).build();
        return propertyMapping;
    }

}