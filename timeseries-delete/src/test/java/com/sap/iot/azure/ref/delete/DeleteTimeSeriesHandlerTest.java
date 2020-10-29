package com.sap.iot.azure.ref.delete;

import com.microsoft.azure.storage.queue.CloudQueueMessage;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesException;
import com.sap.iot.azure.ref.delete.storagequeue.OperationStorageQueueProcessor;
import com.sap.iot.azure.ref.integration.commons.adx.ADXTableManager;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeleteTimeSeriesHandlerTest {
    @Mock
    private ADXTableManager adxTableManager;
    @Mock
    private OperationStorageQueueProcessor operationStorageQueueProcessor;
    @Mock
    private CloudQueueMessage cloudQueueMessage;
    @InjectMocks
    @Spy
    DeleteTimeSeriesHandler deleteTimeSeriesHandler;
    @Captor
    private ArgumentCaptor<List<String>> sourceIdsCaptor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final String REQUEST_ID = "SAMPLE_REQUEST_ID";
    private final String CORRELATION_ID = "SAMPLE_CORRELATION_ID";
    private final String SOURCE_ID = "SAMPLE_SOURCE_ID";
    private final String STRUCTURE_ID = "SAMPLE_STRUCTURE_ID";
    private final String FROM_TIMESTAMP = "2020-01-01T00:00:00.000Z";
    private final String TO_TIMESTAMP = "2020-01-02T00:00:00.000Z";
    private final String INGESTION_TIME = "2020-01-03T00:00:00.000Z";


    @Test
    public void testRun() throws IOException {
        String sampleOperationId = "SAMPLE_OPERATION_ID";

        String message = createDeleteRequest("/DeleteTimeSeriesRequest.json");
        doReturn(sampleOperationId).when(adxTableManager).deleteTimeSeries(any(DeleteInfo.class));
        doReturn(cloudQueueMessage).when(operationStorageQueueProcessor).getOperationInfoMessage(sampleOperationId, REQUEST_ID, STRUCTURE_ID, CORRELATION_ID);

        deleteTimeSeriesHandler.processMessage(message);

        verify(adxTableManager, times(1)).deleteTimeSeries(any(DeleteInfo.class));
        verify(operationStorageQueueProcessor, times(1)).process(cloudQueueMessage, Optional.empty());
    }

    @Test
    public void testInvalidMessage() throws IOException {
        String message = "invalid";

        expectedException.expect(DeleteTimeSeriesException.class);
        deleteTimeSeriesHandler.processMessage(message);
    }

    public String createDeleteRequest(String sampleMessage) throws IOException {
        return String.format(IOUtils.toString(this.getClass().getResourceAsStream(sampleMessage), StandardCharsets.UTF_8), REQUEST_ID,
                CORRELATION_ID, SOURCE_ID, STRUCTURE_ID, INGESTION_TIME, FROM_TIMESTAMP, TO_TIMESTAMP);
    }
}
