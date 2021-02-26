package com.sap.iot.azure.ref.notification.processing.structure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.adx.ADXDataManager;
import com.sap.iot.azure.ref.integration.commons.adx.ADXTableManager;
import com.sap.iot.azure.ref.integration.commons.cache.CacheKeyBuilder;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingHelper;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceLookup;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SchemaWithADXStatus;
import com.sap.iot.azure.ref.notification.exception.NotificationErrorType;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.processing.NotificationMessage;
import com.sap.iot.azure.ref.notification.processing.util.SystemPropertiesGenerator;
import com.sap.iot.azure.ref.notification.util.ModelAbstractionToADXDataTypeMapper;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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
    ADXDataManager adxDataManager;
    @Mock
    CacheRepository cacheRepository;
    @Mock
    MappingHelper mappingHelper;

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
    SystemPropertiesGenerator systemPropertiesGenerator = new SystemPropertiesGenerator();

    @Before
    public void setup() throws IOException {
        structureNotificationProcessor = new StructureNotificationProcessor(mappingServiceLookup, adxTableManager, adxDataManager,
                cacheRepository, mappingHelper);
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
    }

    //test update
    @Test
    public void testPropertyAdd() {
        mockSchemaInfo();
        structureNotificationProcessor.handleUpdate(postNotificationMessage);

        // expected interaction twice - first for storing cache with ADX status as unknown, and then updating the status after ADX update
        assertSchemaUpdate(2);
        verify(adxTableManager, times(1)).updateTableAndMapping(SAMPLE_SCHEMA, SAMPLE_STRUCTURE_ID);
        verify(adxTableManager, times(1)).clearADXTableSchemaCache(SAMPLE_STRUCTURE_ID);
    }

    @Test
    public void testPropertyUpdate() {
        mockSchemaInfo();
        structureNotificationProcessor.handleUpdate(updatePropertyNotificationMessage);

        assertSchemaUpdate(2);
        verify(adxTableManager, times(1)).updateColumn(SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME, ModelAbstractionToADXDataTypeMapper.getADXDataType(SAMPLE_PROPERTY_DATA_TYPE));
        verify(adxTableManager, times(1)).clearADXTableSchemaCache(SAMPLE_STRUCTURE_ID);
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
                verify(adxTableManager, times(1)).clearADXTableSchemaCache(SAMPLE_STRUCTURE_ID);
                reset(adxTableManager);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        //test invalid datatype. No exception should be thrown
        structureNotificationProcessor.handleUpdate(getUpdatePropertyNotificationMessage("INVALID"));
    }

    @Test
    public void testPropertyDelete() {
        mockSchemaInfo();
        doReturn(false).when(adxDataManager).dataExistsForColumn(eq(SAMPLE_STRUCTURE_ID), eq(SAMPLE_PROPERTY_NAME));

        structureNotificationProcessor.handleUpdate(deletePropertyNotificationMessage);

        assertSchemaUpdate(2);
        verify(adxDataManager, times(1)).dataExistsForColumn(SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME);
        verify(adxTableManager, times(1)).dropColumn(SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME, SAMPLE_SCHEMA);
        verify(adxTableManager, times(1)).clearADXTableSchemaCache(SAMPLE_STRUCTURE_ID);
    }

    @Test
    public void testPropertyDeleteWithData() {
        InvocationContextTestUtil.initInvocationContext();
        mockSchemaInfo();
        doReturn(true).when(adxDataManager).dataExistsForColumn(eq(SAMPLE_STRUCTURE_ID), eq(SAMPLE_PROPERTY_NAME));

        structureNotificationProcessor.handleUpdate(deletePropertyNotificationMessage);

        assertSchemaUpdate(2);
        verify(adxDataManager, times(1)).dataExistsForColumn(SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME);
        verify(adxTableManager, times(1)).softDeleteColumn(eq(SAMPLE_STRUCTURE_ID), eq(SAMPLE_PROPERTY_NAME), eq(SAMPLE_SCHEMA));
        verify(adxTableManager, times(1)).clearADXTableSchemaCache(SAMPLE_STRUCTURE_ID);
    }

    @Test
    public void testDeleteStructure() {
        doReturn(false).when(adxDataManager).dataExists(eq(SAMPLE_STRUCTURE_ID));

        structureNotificationProcessor.handleDelete(deleteNotificationMessage);

        verify(adxDataManager, times(1)).dataExists(SAMPLE_STRUCTURE_ID);
        verify(adxTableManager, times(1)).dropTable(SAMPLE_STRUCTURE_ID);
    }

    @Test
    public void testDeleteStructureWithData() {
        InvocationContextTestUtil.initInvocationContext();
        doReturn(true).when(adxDataManager).dataExists(eq(SAMPLE_STRUCTURE_ID));

        structureNotificationProcessor.handleDelete(deleteNotificationMessage);

        verify(cacheRepository, times(1)).delete(CacheKeyBuilder.constructSchemaInfoKey(SAMPLE_STRUCTURE_ID));
        verify(adxDataManager, times(1)).dataExists(SAMPLE_STRUCTURE_ID);
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
        postNotificationMessage.setSource(systemPropertiesGenerator.generateSystemProperties());
        deleteNotificationMessage = mapper.readValue(deleteJson, NotificationMessage.class);
        deleteNotificationMessage.setSource(systemPropertiesGenerator.generateSystemProperties());
        updatePropertyNotificationMessage = mapper.readValue(updatePropertyJson, NotificationMessage.class);
        updatePropertyNotificationMessage.setSource(systemPropertiesGenerator.generateSystemProperties());
        deletePropertyNotificationMessage = mapper.readValue(deletePropertyJson, NotificationMessage.class);
        deletePropertyNotificationMessage.setSource(systemPropertiesGenerator.generateSystemProperties());
    }

    private NotificationMessage getUpdatePropertyNotificationMessage(String dataType) throws IOException {
        NotificationMessage message = mapper.readValue(String.format(IOUtils.toString(StructureNotificationProcessorTest.class.getResourceAsStream(
                "/StructureNotificationMessageUpdateProperty.json"),
                StandardCharsets.UTF_8), SAMPLE_STRUCTURE_ID, SAMPLE_PROPERTY_NAME, dataType), NotificationMessage.class);
        message.setSource(systemPropertiesGenerator.generateSystemProperties());
        return message;
    }

    private void mockSchemaInfo() {
        doReturn(SAMPLE_SCHEMA).when(mappingServiceLookup).getSchemaInfo(eq(SAMPLE_STRUCTURE_ID));
    }

    private void assertSchemaUpdate(int numberOfCacheInteractions) {
        verify(mappingServiceLookup, times(1)).getSchemaInfo(SAMPLE_STRUCTURE_ID);
        verify(mappingHelper, times(numberOfCacheInteractions)).saveSchemaInCache(eq(SAMPLE_STRUCTURE_ID), any(SchemaWithADXStatus.class));
    }
}