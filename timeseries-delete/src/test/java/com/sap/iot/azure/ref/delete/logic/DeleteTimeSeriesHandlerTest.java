package com.sap.iot.azure.ref.delete.logic;

import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.DeleteTimeSeriesTestUtil;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.model.OperationType;
import com.sap.iot.azure.ref.delete.storagequeue.OperationStorageQueueProcessor;
import com.sap.iot.azure.ref.delete.storagequeue.PurgeStorageQueueProcessor;
import com.sap.iot.azure.ref.delete.storagequeue.StorageQueueMessageInfo;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.delete.util.StatusQueueHelper;
import com.sap.iot.azure.ref.integration.commons.adx.ADXDataManager;
import com.sap.iot.azure.ref.integration.commons.avro.TestAVROSchemaConstants;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingHelper;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil.createSystemPropertiesMap;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeleteTimeSeriesHandlerTest {
    @Mock
    private ADXDataManager adxDataManager;
    @Mock
    private OperationStorageQueueProcessor operationStorageQueueProcessor;
    @Mock
    private PurgeStorageQueueProcessor purgeStorageQueueProcessor;
    @Mock
    private MappingHelper mappingHelper;
    @Mock
    private StatusQueueHelper statusQueueHelper;
    @InjectMocks
    @Spy
    DeleteTimeSeriesHandler deleteTimeSeriesHandler;
    @Mock
    private CloudQueueMessage cloudQueueMessage;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @ClassRule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setup() {
        environmentVariables.clear(Constants.IMMEDIATE_PURGE_EXECUTION_NAME);
    }

    @Test
    public void testRun() throws IOException {
        String sampleOperationId = "SAMPLE_OPERATION_ID";

        String message = DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json");
        doReturn(sampleOperationId).when(adxDataManager).deleteTimeSeries(any(DeleteInfo.class));
        doReturn(cloudQueueMessage).when(operationStorageQueueProcessor).getOperationInfoMessage(sampleOperationId, DeleteTimeSeriesTestUtil.REQUEST_ID, DeleteTimeSeriesTestUtil.STRUCTURE_ID, DeleteTimeSeriesTestUtil.CORRELATION_ID,
                OperationType.SOFT_DELETE);
        doReturn(TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA).when(mappingHelper).getSchemaInfo(anyString());

        deleteTimeSeriesHandler.processMessage(message, createSystemPropertiesMap()[0]);

        verify(adxDataManager, times(1)).deleteTimeSeries(any(DeleteInfo.class));
        verify(operationStorageQueueProcessor, times(1)).apply(new StorageQueueMessageInfo(cloudQueueMessage, Optional.empty()));
    }

    @Test
    public void testPurgePII() throws IOException {
        String message = DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json");
        doReturn(TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_GDPR_RELEVANT_PII).when(mappingHelper).getSchemaInfo(anyString());

        deleteTimeSeriesHandler.processMessage(message, createSystemPropertiesMap()[0]);

        verify(purgeStorageQueueProcessor, times(1)).apply(any(CloudQueueMessage.class));
    }

    @Test
    public void testPurgeSPI() throws IOException {
        String message = DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json");
        doReturn(TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_GDPR_RELEVANT_SPI).when(mappingHelper).getSchemaInfo(anyString());

        deleteTimeSeriesHandler.processMessage(message, createSystemPropertiesMap()[0]);

        verify(purgeStorageQueueProcessor, times(1)).apply(any(CloudQueueMessage.class));
    }

    @Test
    public void testPurgeDisableConsolidation() throws IOException {
        environmentVariables.set(Constants.IMMEDIATE_PURGE_EXECUTION_NAME, "true");
        String sampleOperationId = "SAMPLE_OPERATION_ID";

        String message = DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json");
        doReturn(TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_GDPR_RELEVANT_SPI).when(mappingHelper).getSchemaInfo(anyString());
        doReturn(sampleOperationId).when(adxDataManager).purgeTimeSeries(anyString(), any(List.class));
        doReturn(cloudQueueMessage).when(operationStorageQueueProcessor).getOperationInfoMessage(sampleOperationId, DeleteTimeSeriesTestUtil.REQUEST_ID, DeleteTimeSeriesTestUtil.STRUCTURE_ID, DeleteTimeSeriesTestUtil.CORRELATION_ID,
                OperationType.PURGE);

        deleteTimeSeriesHandler.processMessage(message, createSystemPropertiesMap()[0]);

        verify(adxDataManager, times(1)).purgeTimeSeries(anyString(), any(List.class));
        verify(operationStorageQueueProcessor, times(1)).apply(new StorageQueueMessageInfo(cloudQueueMessage, Optional.empty()));
    }

    @Test
    public void testInvalidMessage() {
        String message = "invalid";

        expectedException.expect(DeleteTimeSeriesException.class);
        deleteTimeSeriesHandler.processMessage(message, createSystemPropertiesMap()[0]);
    }

    @Test
    public void testPurgeFailure() throws IOException {
        environmentVariables.set(Constants.IMMEDIATE_PURGE_EXECUTION_NAME, "true");
        ADXClientException testException = new ADXClientException("Test Exception", IdentifierUtil.empty(), false);
        String message = DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json");

        doReturn(TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_GDPR_RELEVANT_SPI).when(mappingHelper).getSchemaInfo(anyString());
        doThrow(testException).when(adxDataManager).purgeTimeSeries(anyString(), any(List.class));

        try {
            deleteTimeSeriesHandler.processMessage(message, createSystemPropertiesMap()[0]);
        }catch (DeleteTimeSeriesException e) {
            verify(statusQueueHelper, times(1)).sendFailedDeleteToStatusQueue(any(DeleteInfo.class), eq(false));
        }
    }

    @Test
    public void testSoftDeleteFailure() throws IOException {
        ADXClientException testException = new ADXClientException("Test Exception", IdentifierUtil.empty(), false);
        String message = DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json");

        doReturn(TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA).when(mappingHelper).getSchemaInfo(anyString());
        doThrow(testException).when(adxDataManager).deleteTimeSeries(any(DeleteInfo.class));

        try {
            deleteTimeSeriesHandler.processMessage(message, createSystemPropertiesMap()[0]);
        }catch (DeleteTimeSeriesException e) {
            verify(statusQueueHelper, times(1)).sendFailedDeleteToStatusQueue(any(DeleteInfo.class), eq(true));
        }
    }

    @Test
    public void testStorageQueueException() throws IOException {
        environmentVariables.set(Constants.IMMEDIATE_PURGE_EXECUTION_NAME, "true");
        String sampleOperationId = "SAMPLE_OPERATION_ID";
        DeleteTimeSeriesException exception =  new DeleteTimeSeriesException("Storage Queue Error", DeleteTimeSeriesErrorType.STORAGE_QUEUE_ERROR,
                IdentifierUtil.empty(), false);

        String message = DeleteTimeSeriesTestUtil.createDeleteRequestWithPlaceHolders("/DeleteTimeSeriesRequest.json");
        doReturn(TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_GDPR_RELEVANT_SPI).when(mappingHelper).getSchemaInfo(anyString());
        doReturn(sampleOperationId).when(adxDataManager).purgeTimeSeries(anyString(), any(List.class));
        doReturn(cloudQueueMessage).when(operationStorageQueueProcessor).getOperationInfoMessage(sampleOperationId, DeleteTimeSeriesTestUtil.REQUEST_ID, DeleteTimeSeriesTestUtil.STRUCTURE_ID, DeleteTimeSeriesTestUtil.CORRELATION_ID,
                OperationType.PURGE);
        doThrow(exception).when(operationStorageQueueProcessor).apply(new StorageQueueMessageInfo(cloudQueueMessage, Optional.empty()));

        expectedException.expect(DeleteTimeSeriesException.class);
        deleteTimeSeriesHandler.processMessage(message, createSystemPropertiesMap()[0]);
    }
}
