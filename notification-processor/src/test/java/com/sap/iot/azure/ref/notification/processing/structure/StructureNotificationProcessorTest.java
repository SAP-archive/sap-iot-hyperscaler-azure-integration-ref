package com.sap.iot.azure.ref.notification.processing.structure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.adx.ADXTableManager;
import com.sap.iot.azure.ref.integration.commons.cache.CacheKeyBuilder;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingHelper;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceLookup;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SchemaWithADXStatus;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;
import com.sap.iot.azure.ref.notification.exception.NotificationErrorType;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.processing.NotificationMessage;
import com.sap.iot.azure.ref.notification.util.Constants;
import com.sap.iot.azure.ref.notification.util.ModelAbstractionToADXDataTypeMapper;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class StructureNotificationProcessorTest {
    @Mock
    MappingServiceLookup mappingServiceLookup;
    @Mock
    ADXTableManager adxTableManager;
    @Mock
    CacheRepository cacheRepository;
    @Mock
    MappingHelper mappingHelper;
    @Spy
    RetryTaskExecutor retryTaskExecutor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final ObjectMapper mapper = new ObjectMapper();
    private final String SAMPLE_STRUCTURE_ID = "strucId";
    private final String SAMPLE_PROPERTY_NAME = "propertyName";
    private final String SAMPLE_PROPERTY_DATA_TYPE = "String";
    private final String SAMPLE_SCHEMA = "schema";

    private StructureNotificationProcessor structureNotificationProcessor;
    private NotificationMessage postNotificationMessage;
    private NotificationMessage deleteNotificationMessage;
    private NotificationMessage updatePropertyNotificationMessage;
    private NotificationMessage deletePropertyNotificationMessage;

    @Before
    public void setup() throws IOException {
        structureNotificationProcessor = new StructureNotificationProcessor(mappingServiceLookup, adxTableManager,
                cacheRepository, mappingHelper, retryTaskExecutor);
        reset(mappingHelper);
        reset(mappingServiceLookup);
        reset(adxTableManager);

        mockNotificationMessages();
    }

    //test create
    @Test
    public void testCreate() {
        doReturn(SAMPLE_SCHEMA).when(mappingServiceLookup).getSchemaInfo(eq(SAMPLE_STRUCTURE_ID));
        structureNotificationProcessor.handleCreate(postNotificationMessage);

        verify(mappingServiceLookup, times(1)).getSchemaInfo(SAMPLE_STRUCTURE_ID);
        verify(mappingHelper, times(2)).saveSchemaInCache(eq(SAMPLE_STRUCTURE_ID), any());
        verify(retryTaskExecutor, times(2)).executeWithRetry(any(), eq(Constants.MAX_RETRIES));
    }

    //test update
    @Test
    public void testPropertyAdd() {
        mockSchemaInfo();
        structureNotificationProcessor.handleUpdate(postNotificationMessage);

        // expected interaction twice - first for storing cache with ADX status as unknown, and then updating the status after ADX update
        assertSchemaUpdate(2);
        verify(adxTableManager, times(1)).updateTableAndMapping(SAMPLE_SCHEMA, SAMPLE_STRUCTURE_ID);
        verify(retryTaskExecutor, times(2)).executeWithRetry(any(), eq(Constants.MAX_RETRIES));
    }

    @Test
    public void testPropertyUpdate() {
        mockSchemaInfo();
        structureNotificationProcessor.handleUpdate(updatePropertyNotificationMessage);

        assertSchemaUpdate(2);
        verify(adxTableManager, times(1)).updateColumn(SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME, ModelAbstractionToADXDataTypeMapper.getADXDataType(SAMPLE_PROPERTY_DATA_TYPE));
        verify(retryTaskExecutor, times(2)).executeWithRetry(any(), eq(Constants.MAX_RETRIES));
    }

    @Test
    public void testUpdateDataTypes() throws IOException {
        mockSchemaInfo();
        List<String> datatypes = new ArrayList<>();
        datatypes.add("String");
        datatypes.add("LargeString");
        datatypes.add("Numeric");
        datatypes.add("NumericFlexible");
        datatypes.add("Timestamp");
        datatypes.add("DateTime");
        datatypes.add("Date");
        datatypes.add("Boolean");
        datatypes.add("JSON");

        datatypes.forEach(datatype -> {
            try {
                structureNotificationProcessor.handleUpdate(getUpdatePropertyNotificationMessage(datatype));

                verify(adxTableManager, times(1)).updateColumn(SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME,
                        ModelAbstractionToADXDataTypeMapper.getADXDataType(datatype));
                reset(adxTableManager);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        //test invalid datatype. No excpetion should be thrown
        structureNotificationProcessor.handleUpdate(getUpdatePropertyNotificationMessage("INVALID"));
    }

    @Test
    public void testPropertyDelete() {
        mockSchemaInfo();
        doReturn(false).when(adxTableManager).dataExistsForColumn(eq(SAMPLE_STRUCTURE_ID), eq(SAMPLE_PROPERTY_NAME));

        structureNotificationProcessor.handleUpdate(deletePropertyNotificationMessage);

        assertSchemaUpdate(2);
        verify(adxTableManager, times(1)).dataExistsForColumn(SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME);
        verify(adxTableManager, times(1)).dropColumn(SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME, SAMPLE_SCHEMA);
        verify(retryTaskExecutor, times(2)).executeWithRetry(any(), eq(Constants.MAX_RETRIES));
    }

    @Test
    public void testPropertyDeleteWithData() {
        InvocationContextTestUtil.initInvocationContext();
        mockSchemaInfo();
        doReturn(true).when(adxTableManager).dataExistsForColumn(eq(SAMPLE_STRUCTURE_ID), eq(SAMPLE_PROPERTY_NAME));

        structureNotificationProcessor.handleUpdate(deletePropertyNotificationMessage);

        assertSchemaUpdate(2);
        verify(adxTableManager, times(1)).dataExistsForColumn(SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME);
        verify(adxTableManager, times(1)).softDeleteColumn(eq(SAMPLE_STRUCTURE_ID), eq(SAMPLE_PROPERTY_NAME), eq(SAMPLE_SCHEMA));
    }

    @Test
    public void testDeleteStructure() {
        doReturn(false).when(adxTableManager).dataExists(eq(SAMPLE_STRUCTURE_ID));

        structureNotificationProcessor.handleDelete(deleteNotificationMessage);

        verify(adxTableManager, times(1)).dataExists(SAMPLE_STRUCTURE_ID);
        verify(adxTableManager, times(1)).dropTable(SAMPLE_STRUCTURE_ID);
        verify(retryTaskExecutor, times(1)).executeWithRetry(any(), eq(Constants.MAX_RETRIES));
    }

    @Test
    public void testDeleteStructureWithData() {
        InvocationContextTestUtil.initInvocationContext();
        doReturn(true).when(adxTableManager).dataExists(eq(SAMPLE_STRUCTURE_ID));

        structureNotificationProcessor.handleDelete(deleteNotificationMessage);

        verify(cacheRepository, times(1)).delete(CacheKeyBuilder.constructSchemaInfoKey(SAMPLE_STRUCTURE_ID));
        verify(adxTableManager, times(1)).dataExists(SAMPLE_STRUCTURE_ID);
        verify(adxTableManager, times(1)).softDeleteTable(SAMPLE_STRUCTURE_ID);
    }

    @Test
    public void testPermanentExceptions() {
        NotificationProcessException notificationProcessException = new NotificationProcessException("Sample", NotificationErrorType.DATA_TYPE_ERROR,
                IdentifierUtil.empty(), false);
        doThrow(notificationProcessException).when(mappingServiceLookup).getSchemaInfo(any());
        doThrow(notificationProcessException).when(cacheRepository).delete(any());

        //Since we only log the error, no exception should be thrown
        structureNotificationProcessor.handleCreate(postNotificationMessage);
        structureNotificationProcessor.handleUpdate(postNotificationMessage);
        structureNotificationProcessor.handleDelete(deleteNotificationMessage);
    }

    private void mockNotificationMessages() throws IOException {
        String postJson = String.format(IOUtils.toString(StructureNotificationProcessorTest.class.getResourceAsStream("/StructureNotificationMessageCreate.json"),
                StandardCharsets.UTF_8), SAMPLE_STRUCTURE_ID);
        String deleteJson = String.format(IOUtils.toString(StructureNotificationProcessorTest.class.getResourceAsStream("/StructureNotificationMessageDelete.json"),
                StandardCharsets.UTF_8), SAMPLE_STRUCTURE_ID);
        String updatePropertyJson = String.format(IOUtils.toString(StructureNotificationProcessorTest.class.getResourceAsStream(
                "/StructureNotificationMessageUpdateProperty.json"),
                StandardCharsets.UTF_8), SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME, SAMPLE_PROPERTY_DATA_TYPE);
        String deletePropertyJson = String.format(IOUtils.toString(StructureNotificationProcessorTest.class.getResourceAsStream(
                "/StructureNotificationMessageDeleteProperty.json"),
                StandardCharsets.UTF_8), SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME);

        postNotificationMessage = mapper.readValue(postJson, NotificationMessage.class);
        deleteNotificationMessage = mapper.readValue(deleteJson, NotificationMessage.class);
        updatePropertyNotificationMessage = mapper.readValue(updatePropertyJson, NotificationMessage.class);
        deletePropertyNotificationMessage = mapper.readValue(deletePropertyJson, NotificationMessage.class);
    }

    private NotificationMessage getUpdatePropertyNotificationMessage(String dataType) throws IOException {
        return mapper.readValue(String.format(IOUtils.toString(StructureNotificationProcessorTest.class.getResourceAsStream(
                "/StructureNotificationMessageUpdateProperty.json"),
                StandardCharsets.UTF_8), SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME, dataType), NotificationMessage.class);
    }

    private void mockSchemaInfo() {
        doReturn(SAMPLE_SCHEMA).when(mappingServiceLookup).getSchemaInfo(eq(SAMPLE_STRUCTURE_ID));
    }

    private void assertSchemaUpdate(int numberOfCacheInteractions) {
        verify(mappingServiceLookup, times(1)).getSchemaInfo(SAMPLE_STRUCTURE_ID);
        verify(mappingHelper, times(numberOfCacheInteractions)).saveSchemaInCache(eq(SAMPLE_STRUCTURE_ID), any(SchemaWithADXStatus.class));
    }
}