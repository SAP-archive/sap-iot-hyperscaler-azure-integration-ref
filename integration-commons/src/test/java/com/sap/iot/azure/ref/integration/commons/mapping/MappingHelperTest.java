package com.sap.iot.azure.ref.integration.commons.mapping;

import com.sap.iot.azure.ref.integration.commons.adx.ADXTableManager;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.model.mapping.SensorMappingInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMapping;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMappingInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SchemaWithADXStatus;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorAssignment;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.Tag;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MappingHelperTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mock
    private MappingServiceLookup mappingServiceLookup;
    @Mock
    private CacheRepository cacheRepository;
    @Mock
    private ADXTableManager adxTableManager;

    private MappingHelper mappingHelper;

    private final String SAMPLE_SENSOR_ID = "sensorId";
    private final String SAMPLE_VIRTUAL_CAPABILITY_ID = "virtualCapabilityId";
    private final String SAMPLE_SOURCE_ID = "sourceId";
    private final String SAMPLE_STRUCTURE_ID = "structureId";
    private final String SAMPLE_TAG_SEMANTIC = "tagSemantic";
    private final String SAMPLE_TAG_VALUE = "tagValue";
    private final String SAMPLE_MAPPING_ID = "mappingId";
    private final String SAMPLE_STRUCTURE_PROPERTY_ID = "sampleStructureId";
    private final String SAMPLE_CAPABILITY_PROPERTY_ID = "sampleCapabilityId";

    @BeforeClass
    public static void setupClass() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Before
    public void setup() {
        InvocationContextTestUtil.initInvocationContext();
        mappingHelper = new MappingHelper(mappingServiceLookup, cacheRepository, adxTableManager);
    }

    @After
    public void teardown() {
        reset(mappingServiceLookup);
        reset(cacheRepository);
        reset(adxTableManager);
    }

    @Test
    public void testCachedInfo() {
        String separator = MappingServiceConstants.CACHE_KEY_SEPARATOR;
        String cacheKeyCreatorPrefix = MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX;
        String sensorKeyPrefix = MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX;
        String mappingKeyPrefix = MappingServiceConstants.CACHE_MAPPING_KEY_PREFIX;
        String structureKeyPrefix = MappingServiceConstants.CACHE_STRUCTURE_KEY_PREFIX;
        SensorMappingInfo expected = SensorMappingInfo.builder()
                .sourceId(SAMPLE_SOURCE_ID)
                .structureId(SAMPLE_STRUCTURE_ID)
                .tags(Collections.singletonList(Tag.builder().tagSemantic(SAMPLE_TAG_SEMANTIC).tagValue(SAMPLE_TAG_VALUE).build()))
                .propertyMappings(Collections.singletonList(PropertyMapping.builder().capabilityPropertyId(SAMPLE_CAPABILITY_PROPERTY_ID).structurePropertyId(SAMPLE_STRUCTURE_PROPERTY_ID).build()))
                .schemaInfo("").build();

        //test cached info
        doReturn(Optional.of(getSampleDeviceInfo())).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        doReturn(Optional.of(getSamplePropertyMappingInfo())).when(cacheRepository).get(any(byte[].class), eq(PropertyMappingInfo.class));
        doReturn(Optional.of(new SchemaWithADXStatus(getSampleSchemaInfo(), true))).when(cacheRepository).get(any(byte[].class),
                eq(SchemaWithADXStatus.class));

        SensorMappingInfo sensorMappingInfo = mappingHelper.getSensorMapping(SAMPLE_SENSOR_ID, SAMPLE_VIRTUAL_CAPABILITY_ID);

        // Cache info fetched with correct keys
        verify(cacheRepository, times(1)).get(eq((cacheKeyCreatorPrefix + sensorKeyPrefix + SAMPLE_SENSOR_ID + separator + SAMPLE_VIRTUAL_CAPABILITY_ID).getBytes()),
                eq(SensorInfo.class));
        verify(cacheRepository, times(1)).get(eq((cacheKeyCreatorPrefix + mappingKeyPrefix + SAMPLE_MAPPING_ID + separator + SAMPLE_STRUCTURE_ID + separator + SAMPLE_VIRTUAL_CAPABILITY_ID).getBytes()), eq(PropertyMappingInfo.class));

        // since the adx sync status is already true, there's only one interaction with schema
        verify(cacheRepository, times(1)).get(eq((cacheKeyCreatorPrefix + structureKeyPrefix + SAMPLE_STRUCTURE_ID).getBytes()),
                eq(SchemaWithADXStatus.class));

        // since the adx sync status is already true, verify no interactions with ADX
        verifyZeroInteractions(adxTableManager);

        //Returned Mapping info should contain the values fetched from cache
        assertEquals(expected, sensorMappingInfo);
    }

    @Test
    public void testADXResourceCreation() {
        String sampleSchemaInfo = getSampleSchemaInfo();

        doReturn(Optional.of(getSampleDeviceInfo())).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        doReturn(Optional.of(getSamplePropertyMappingInfo())).when(cacheRepository).get(any(byte[].class), eq(PropertyMappingInfo.class));
        doReturn(Optional.of(sampleSchemaInfo)).when(cacheRepository).get(any(byte[].class), eq(String.class));
        doReturn(Optional.of(true)).when(cacheRepository).get(any(byte[].class), eq(boolean.class));

        //If DeviceInfo retrieved from cache, ensureThatADXResourcesExist should not be invoked
        mappingHelper.getSensorMapping("", "");
        verify(adxTableManager, times(0)).checkIfExists(anyString(), anyString());

        //If DeviceInfo not found in cache, ensureThatADXResourcesExist should be invoked
        doReturn(Optional.empty()).when(cacheRepository).get(any(byte[].class), eq(String.class));
        doReturn(sampleSchemaInfo).when(mappingServiceLookup).getSchemaInfo(anyString());
        mappingHelper.getSensorMapping("", "");
        verify(adxTableManager, times(1)).checkIfExists(anyString(), anyString());
    }

    @Test
    public void testFailedADXResourceCreation() {
        String sampleSchemaInfo = getSampleSchemaInfo();

        SensorInfo sampleSensorInfo = SensorInfo.builder()
                .mappingId("m1")
                .sensorId("s1")
                .virtualCapabilityId("c1")
                .sourceId("src1")
                .structureId("struc1")
                .tags(Collections.singletonList(new Tag("key1", "value1")))
                .build();

        doReturn(Optional.of(sampleSensorInfo)).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        doReturn(Optional.of(getSamplePropertyMappingInfo())).when(cacheRepository).get(any(byte[].class), eq(PropertyMappingInfo.class));
        doReturn(Optional.of(new SchemaWithADXStatus(sampleSchemaInfo, false))).when(cacheRepository).get(any(byte[].class),
                eq(SchemaWithADXStatus.class));

        //If failed adx table creation flag found in cache, checkIfExists should be invoked
        mappingHelper.getSensorMapping("", "");
        verify(adxTableManager, times(1)).checkIfExists(anyString(), anyString());
    }

    @Test
    public void testAPIInvocation() {
        doReturn(Optional.empty()).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        doReturn(getSampleDeviceInfo()).when(mappingServiceLookup).getSensorInfo(anyString(), anyString(), any());
        doReturn(Optional.empty()).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        doReturn(getSamplePropertyMappingInfos()).when(mappingServiceLookup).getPropertyMappingInfos(anyString());
        doReturn(Optional.empty()).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        doReturn(getSampleSchemaInfo()).when(mappingServiceLookup).getSchemaInfo(anyString());

        mappingHelper.getSensorMapping(SAMPLE_SENSOR_ID, SAMPLE_VIRTUAL_CAPABILITY_ID);

        //If mapping info not found in cache, it will be fetched from mappingServiceLookup
        verify(mappingServiceLookup, times(1)).getSensorInfo(SAMPLE_SENSOR_ID, SAMPLE_VIRTUAL_CAPABILITY_ID, Optional.empty());
        verify(mappingServiceLookup, times(1)).getPropertyMappingInfos(SAMPLE_MAPPING_ID);
        verify(mappingServiceLookup, times(1)).getSchemaInfo(SAMPLE_STRUCTURE_ID);
    }

    @Test
    public void testGetDeviceMappingWithSensorAssignmentCache() {
        doReturn(Optional.empty()).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        doReturn(getSampleDeviceInfo()).when(mappingServiceLookup).getSensorInfo(anyString(), anyString(), any());
        doReturn(Optional.empty()).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        doReturn(getSamplePropertyMappingInfos()).when(mappingServiceLookup).getPropertyMappingInfos(anyString());
        doReturn(Optional.empty()).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        doReturn(getSampleSchemaInfo()).when(mappingServiceLookup).getSchemaInfo(anyString());
        when(mappingHelper.fetchSensorAssignmentInfoFromCache(SAMPLE_SENSOR_ID)).thenReturn(getSensorAssignmentCacheInfo());

        mappingHelper.getSensorMapping(SAMPLE_SENSOR_ID, SAMPLE_VIRTUAL_CAPABILITY_ID);

        //If mapping info not found in cache, it will be fetched from mappingServiceLookup
        verify(mappingServiceLookup, times(1)).getSensorInfo(SAMPLE_SENSOR_ID, SAMPLE_VIRTUAL_CAPABILITY_ID, getSensorAssignmentCacheInfo());
        verify(mappingServiceLookup, times(1)).getPropertyMappingInfos(SAMPLE_MAPPING_ID);
        verify(mappingServiceLookup, times(1)).getSchemaInfo(SAMPLE_STRUCTURE_ID);
    }

    @Test
    public void testIoTRuntimeException() {
        IoTRuntimeException sampleException = new IoTRuntimeException("", CommonErrorType.MAPPING_LOOKUP_ERROR, "",
                IdentifierUtil.getIdentifier("testKey", "testValue"),
                true);

        doReturn(Optional.empty()).when(cacheRepository).get(any(byte[].class), eq(SensorInfo.class));
        Mockito.doThrow(sampleException).when(mappingServiceLookup).getSensorInfo(anyString(), anyString(), any());

        expectedException.expect(IoTRuntimeException.class);
        mappingHelper.getSensorMapping(SAMPLE_SENSOR_ID, SAMPLE_VIRTUAL_CAPABILITY_ID);
    }

    private Optional<SensorAssignment> getSensorAssignmentCacheInfo() {
        String SAMPLE_SENSOR_ID = "sampleSensorId";
        String SAMPLE_ASSIGNMENT_ID = "sampleAssignmentId";
        String SAMPLE_OBJECT_ID = "sampleObjectId";
        String SAMPLE_MAPPING_ID = "sampleMappingId";
        SensorAssignment sensorAssignmentCacheInfo = new SensorAssignment(SAMPLE_SENSOR_ID, SAMPLE_ASSIGNMENT_ID, SAMPLE_MAPPING_ID, SAMPLE_OBJECT_ID);
        return Optional.of(sensorAssignmentCacheInfo);
    }

    private SensorInfo getSampleDeviceInfo() {
        return SensorInfo.builder()
                .sensorId(SAMPLE_SENSOR_ID)
                .virtualCapabilityId(SAMPLE_VIRTUAL_CAPABILITY_ID)
                .sourceId(SAMPLE_SOURCE_ID)
                .structureId(SAMPLE_STRUCTURE_ID)
                .tags(getSampleTags())
                .mappingId(SAMPLE_MAPPING_ID)
                .build();
    }

    private List<Tag> getSampleTags() {
        return Arrays.asList(Tag.builder()
                .tagSemantic(SAMPLE_TAG_SEMANTIC)
                .tagValue(SAMPLE_TAG_VALUE)
                .build());
    }

    private PropertyMappingInfo getSamplePropertyMappingInfo() {
        return PropertyMappingInfo.builder()
                .mappingId(SAMPLE_MAPPING_ID)
                .structureId(SAMPLE_STRUCTURE_ID)
                .virtualCapabilityId(SAMPLE_VIRTUAL_CAPABILITY_ID)
                .propertyMappings(getSamplePropertyMappings())
                .build();
    }

    private List<PropertyMappingInfo> getSamplePropertyMappingInfos() {
        boolean[] structureIdDifferentValues = {true, false};
        boolean[] capabilityIdDifferentValues = {true, false};
        List<PropertyMappingInfo> result = new ArrayList<>();

        for (boolean structureIdDifferent : structureIdDifferentValues) {
            for (boolean capabilityIdDifferent : capabilityIdDifferentValues) {
                String structureId = SAMPLE_STRUCTURE_ID;
                String capabilityId = SAMPLE_VIRTUAL_CAPABILITY_ID;

                if (structureIdDifferent) {
                    structureId += "DIFFERENT";
                }
                if (capabilityIdDifferent) {
                    capabilityId += "DIFFERENT";
                }
                result.add(PropertyMappingInfo.builder()
                        .mappingId(SAMPLE_MAPPING_ID)
                        .structureId(structureId)
                        .virtualCapabilityId(capabilityId)
                        .propertyMappings(getSamplePropertyMappings())
                        .build());
            }
        }

        return result;
    }

    private List<PropertyMapping> getSamplePropertyMappings() {
        return Arrays.asList(PropertyMapping.builder()
                .structurePropertyId(SAMPLE_STRUCTURE_PROPERTY_ID)
                .capabilityPropertyId(SAMPLE_CAPABILITY_PROPERTY_ID)
                .build());
    }

    private String getSampleSchemaInfo() {
        return "";
    }

}