package com.sap.iot.azure.ref.integration.commons.mapping;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.MappingLookupException;
import com.sap.iot.azure.ref.integration.commons.avro.TestAVROSchemaConstants;
import com.sap.iot.azure.ref.integration.commons.mapping.token.TenantTokenCache;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMapping;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMappingInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorAssignment;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.Tag;
import org.apache.http.HttpStatus;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MappingServiceLookupTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables()
            .set(MappingServiceConstants.TENANT_PROP, MappingServiceLookupTestConstants.SAMPLE_TENANT);
    @Mock
    private AsyncHttpClient asyncHttpClient;
    @Mock
    private TenantTokenCache tenantTokenCache;

    private MappingServiceLookup mappingServiceLookup;

    @BeforeClass
    public static void classSetup() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Before
    public void setup() {
        mappingServiceLookup = new MappingServiceLookup(asyncHttpClient, tenantTokenCache);
    }

    @Test
    public void testGetSensorInfo() throws ExecutionException, InterruptedException {
        String tagUrl = MappingServiceConstants.TAGS_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID).replace(MappingServiceConstants.VIRTUAL_CAPABILITY_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1);
        String assignmentUrl = MappingServiceConstants.ASSIGNMENT_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID);
        SensorInfo expected = SensorInfo.builder()
                .sensorId(MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID)
                .virtualCapabilityId(MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1)
                .sourceId(MappingServiceLookupTestConstants.SAMPLE_SOURCE_ID)
                .structureId(MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_ID_1)
                .mappingId(MappingServiceLookupTestConstants.SAMPLE_MAPPING_ID)
                .tags(Arrays.asList(
                        Tag.builder()
                                .tagValue(MappingServiceLookupTestConstants.SAMPLE_TAG_VALUE_1)
                                .tagSemantic(MappingServiceLookupTestConstants.SAMPLE_TAG_SEMANTIC_1)
                                .build(),
                        Tag.builder()
                                .tagValue(MappingServiceLookupTestConstants.SAMPLE_TAG_VALUE_2)
                                .tagSemantic(MappingServiceLookupTestConstants.SAMPLE_TAG_SEMANTIC_2)
                                .build()
                )).build();

        //mock tags api
        mockHttpClientPayload(tagUrl, MappingServiceLookupTestConstants.SAMPLE_TAG_PAYLOAD);
        //mock assignment api
        mockHttpClientPayload(assignmentUrl, String.format(MappingServiceLookupTestConstants.SAMPLE_ASSIGNMENT_PAYLOAD, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID));

        // verify returned sensor info
        Assert.assertEquals(expected, mappingServiceLookup.getSensorInfo(MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID,
                MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1, getSensorAssignmentCacheInfo()));
    }

    @Test
    public void testGetSensorInfoWithInvalidTagPayload() throws ExecutionException, InterruptedException {
        String tagUrl = MappingServiceConstants.TAGS_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID).replace(MappingServiceConstants.VIRTUAL_CAPABILITY_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1);
        String assignmentUrl = MappingServiceConstants.ASSIGNMENT_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID);

        //mock tags api
        mockHttpClientPayload(tagUrl, "INVALID");
        //mock assignment api
        mockHttpClientPayload(assignmentUrl, String.format(MappingServiceLookupTestConstants.SAMPLE_ASSIGNMENT_PAYLOAD, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID));

        expectedException.expect(MappingLookupException.class);
        mappingServiceLookup.getSensorInfo(MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1, getSensorAssignmentCacheInfo());
    }

    @Test
    public void testGetSensorInfoWithTagLookupExecutionException() throws ExecutionException, InterruptedException {
        mockExceptionHttpClient(new ExecutionException(new Exception()));
        expectedException.expect(MappingLookupException.class);
        mappingServiceLookup.getSensorInfo(MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1, getSensorAssignmentCacheInfo());
    }

    @Test
    public void testGetSensorInfoWithTagLookupInterruptedException() throws ExecutionException, InterruptedException {
        mockExceptionHttpClient(new InterruptedException());
        try {
            mappingServiceLookup.getSensorInfo(MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1, getSensorAssignmentCacheInfo());
        }catch (MappingLookupException e) {
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testGetSensorInfoWithInvalidAssignmentPayload() throws ExecutionException, InterruptedException {
        String tagUrl = MappingServiceConstants.TAGS_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID).replace(MappingServiceConstants.VIRTUAL_CAPABILITY_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1);
        String assignmentUrl = MappingServiceConstants.ASSIGNMENT_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID);

        //mock tags api
        mockHttpClientPayload(tagUrl, MappingServiceLookupTestConstants.SAMPLE_TAG_PAYLOAD);
        //mock assignment api
        mockHttpClientPayload(assignmentUrl, "INVALID");

        // verify returned sensor info
        expectedException.expect(MappingLookupException.class);
        mappingServiceLookup.getSensorInfo(MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1,
                Optional.empty());
    }

    @Test
    public void testGetSensorInfoWithAssignmentLookupExecutionException() throws ExecutionException, InterruptedException {
        String tagUrl = MappingServiceConstants.TAGS_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER,
                MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID).replace(MappingServiceConstants.VIRTUAL_CAPABILITY_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1);
        String assignmentUrl = MappingServiceConstants.ASSIGNMENT_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID);

        //mock tags api
        mockHttpClientPayload(tagUrl, MappingServiceLookupTestConstants.SAMPLE_TAG_PAYLOAD);
        mockExceptionHttpClient(assignmentUrl, new ExecutionException(new Exception()));

        expectedException.expect(MappingLookupException.class);
        mappingServiceLookup.getSensorInfo(MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1,
                Optional.empty());
    }

    @Test
    public void testGetSensorInfoWithAssignmentLookupInterruptedException() throws ExecutionException, InterruptedException {
        String tagUrl = MappingServiceConstants.TAGS_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER,
                MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID).replace(MappingServiceConstants.VIRTUAL_CAPABILITY_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1);
        String assignmentUrl = MappingServiceConstants.ASSIGNMENT_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID);

        //mock tags api
        mockHttpClientPayload(tagUrl, MappingServiceLookupTestConstants.SAMPLE_TAG_PAYLOAD);
        mockExceptionHttpClient(assignmentUrl, new InterruptedException());

        try {
            mappingServiceLookup.getSensorInfo(MappingServiceLookupTestConstants.SAMPLE_SENSOR_ID, MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1,
                    Optional.empty());
        }catch (MappingLookupException e) {
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testGetPropertyMappingInfos() throws ExecutionException, InterruptedException {
        String mappingUrl = MappingServiceConstants.MAPPING_ENDPOINT.replace(MappingServiceConstants.MAPPING_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_MAPPING_ID);
        List<PropertyMappingInfo> expected = Arrays.asList(
                PropertyMappingInfo.builder()
                        .mappingId(MappingServiceLookupTestConstants.SAMPLE_MAPPING_ID)
                        .structureId(MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_ID_1)
                        .virtualCapabilityId(MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_1)
                        .propertyMappings(Arrays.asList(
                                PropertyMapping.builder()
                                        .structurePropertyId(MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_PROPERTY_ID_1)
                                        .capabilityPropertyId(MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_PROPERTY_ID_1)
                                        .build()
                        ))
                        .build(),
                PropertyMappingInfo.builder()
                        .mappingId(MappingServiceLookupTestConstants.SAMPLE_MAPPING_ID)
                        .structureId(MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_ID_2)
                        .virtualCapabilityId(MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_ID_2)
                        .propertyMappings(Arrays.asList(
                                PropertyMapping.builder()
                                        .structurePropertyId(MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_PROPERTY_ID_2)
                                        .capabilityPropertyId(MappingServiceLookupTestConstants.SAMPLE_CAPABILITY_PROPERTY_ID_2)
                                        .build()
                        ))
                        .build()
        );
        //mock tags api
        mockHttpClientPayload(mappingUrl, MappingServiceLookupTestConstants.SAMPLE_MAPPING_PAYLOAD);

        assertEquals(expected, mappingServiceLookup.getPropertyMappingInfos(MappingServiceLookupTestConstants.SAMPLE_MAPPING_ID));
    }

    @Test
    public void testGetPropertyMappingInfosWithExecutionException() throws ExecutionException, InterruptedException {
        //execution exception
        mockExceptionHttpClient(new ExecutionException(new Exception()));
        expectedException.expect(MappingLookupException.class);
        mappingServiceLookup.getPropertyMappingInfos(MappingServiceLookupTestConstants.SAMPLE_MAPPING_ID);
    }

    @Test
    public void testGetPropertyMappingInfosWithInterruptedException() throws ExecutionException, InterruptedException {
        //interrupted exception
        mockExceptionHttpClient(new InterruptedException());

        try {
            mappingServiceLookup.getPropertyMappingInfos(MappingServiceLookupTestConstants.SAMPLE_MAPPING_ID);
        }catch (MappingLookupException e) {
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testGetPropertyMappingInfosWithInvalidPropertyMappings() throws ExecutionException, InterruptedException {
        String mappingUrl = MappingServiceConstants.MAPPING_ENDPOINT.replace(MappingServiceConstants.MAPPING_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_MAPPING_ID);

        //mock tags api
        mockHttpClientPayload(mappingUrl, "INVALID");

        expectedException.expect(MappingLookupException.class);
        mappingServiceLookup.getPropertyMappingInfos(MappingServiceLookupTestConstants.SAMPLE_MAPPING_ID);
    }

    @Test
    public void testGetSchemaInfo() throws ExecutionException, InterruptedException {
        String schemaUrl = MappingServiceConstants.SCHEMA_ENDPOINT.replace(MappingServiceConstants.STRUCTURE_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_ID_1);

        //mock tags api
        mockHttpClientPayload(schemaUrl, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA);

        assertEquals(TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA, mappingServiceLookup.getSchemaInfo(MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_ID_1));
    }

    @Test
    public void testGetSchemaInfoWithInvalidSchema() throws ExecutionException, InterruptedException {
        String schemaUrl = MappingServiceConstants.SCHEMA_ENDPOINT.replace(MappingServiceConstants.STRUCTURE_ID_PLACEHOLDER, MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_ID_1);

        //mock tags api
        mockHttpClientPayload(schemaUrl, TestAVROSchemaConstants.SAMPLE_INVALID_AVRO_SCHEMA);

        expectedException.expect(MappingLookupException.class);
        mappingServiceLookup.getSchemaInfo(MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_ID_1);
    }

    @Test
    public void testGetSchemaInfoWithExecutionException() throws ExecutionException, InterruptedException {
        mockExceptionHttpClient(new ExecutionException(new Exception()));
        expectedException.expect(MappingLookupException.class);
        mappingServiceLookup.getSchemaInfo(MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_ID_1);
    }

    @Test
    public void testGetSchemaInfoWithInterruptedException() throws ExecutionException, InterruptedException {
        mockExceptionHttpClient(new InterruptedException());
        try {
            mappingServiceLookup.getSchemaInfo(MappingServiceLookupTestConstants.SAMPLE_STRUCTURE_ID_1);
        }catch (MappingLookupException e) {
            assertTrue(Thread.interrupted());
        }
    }

    private Optional<SensorAssignment> getSensorAssignmentCacheInfo() {
        String SAMPLE_SENSOR_ID = "sampleSensorId";
        String SAMPLE_ASSIGNMENT_ID = "sampleAssignmentId";
        String SAMPLE_OBJECT_ID = "sampleObjectId";
        String SAMPLE_MAPPING_ID = "sampleMappingId";
        SensorAssignment sensorAssignmentCacheInfo = new SensorAssignment(SAMPLE_SENSOR_ID, SAMPLE_ASSIGNMENT_ID, SAMPLE_MAPPING_ID, SAMPLE_OBJECT_ID);
        return Optional.of(sensorAssignmentCacheInfo);
    }

    private void mockHttpClientPayload(String url, String payload) throws ExecutionException, InterruptedException {
        BoundRequestBuilder mockRequestBuilder = mock(BoundRequestBuilder.class);
        ListenableFuture mockFuture = mock(ListenableFuture.class);
        Response mockResponse = mock(Response.class);

        doReturn(mockRequestBuilder).when(asyncHttpClient).prepareGet(eq(url));
        doReturn(mockRequestBuilder).when(mockRequestBuilder).setHeader(anyString(), anyString());
        doReturn(mockFuture).when(mockRequestBuilder).execute();
        doReturn(mockResponse).when(mockFuture).get();
        doReturn(payload).when(mockResponse).getResponseBody();
        doReturn(HttpStatus.SC_OK).when(mockResponse).getStatusCode();
    }

    private void mockExceptionHttpClient(Exception exception) throws ExecutionException, InterruptedException {
        BoundRequestBuilder mockRequestBuilder = mock(BoundRequestBuilder.class);
        ListenableFuture mockFuture = mock(ListenableFuture.class);

        doReturn(mockRequestBuilder).when(asyncHttpClient).prepareGet(anyString());
        doReturn(mockRequestBuilder).when(mockRequestBuilder).setHeader(anyString(), anyString());
        doReturn(mockFuture).when(mockRequestBuilder).execute();
        doThrow(exception).when(mockFuture).get();
    }

    private void mockExceptionHttpClient(String url, Exception exception) throws ExecutionException, InterruptedException {
        BoundRequestBuilder mockRequestBuilder = mock(BoundRequestBuilder.class);
        ListenableFuture mockFuture = mock(ListenableFuture.class);

        doReturn(mockRequestBuilder).when(asyncHttpClient).prepareGet(eq(url));
        doReturn(mockRequestBuilder).when(mockRequestBuilder).setHeader(anyString(), anyString());
        doReturn(mockFuture).when(mockRequestBuilder).execute();
        doThrow(exception).when(mockFuture).get();
    }

}