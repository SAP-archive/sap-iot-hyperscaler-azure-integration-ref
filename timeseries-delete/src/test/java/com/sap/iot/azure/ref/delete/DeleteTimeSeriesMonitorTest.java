package com.sap.iot.azure.ref.delete;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.delete.model.DeleteMonitoringCloudQueueMessage;
import com.sap.iot.azure.ref.delete.model.OperationInfo;
import com.sap.iot.azure.ref.delete.storagequeue.DeleteMonitoringProcessor;
import com.sap.iot.azure.ref.delete.util.HostConfig;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DeleteTimeSeriesMonitorTest {
    @Mock
    private DeleteMonitoringProcessor deleteMonitoringProcessor;
    @Mock
    private HostConfig hostConfig;

    @InjectMocks
    DeleteTimeSeriesMonitor deleteTimeSeriesMonitor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    DeleteMonitoringCloudQueueMessage deleteMonitoringCloudQueueMessage;
    private static final ObjectMapper mapper = new ObjectMapper();
    OperationInfo operationInfo = new OperationInfo();

    @Before
    public void setup() {
        deleteTimeSeriesMonitor = new DeleteTimeSeriesMonitor(deleteMonitoringProcessor, hostConfig);
        InvocationContextTestUtil.initInvocationContext();
    }

    private static final String OPERATION_ID = "SAMPLE_OPERATION_ID";
    private static final String CORRELATION_ID = "SAMPLE_CORRELATION_ID";
    private static final String EVENT_ID = "SAMPLE_EVENT_ID";
    private static final String STRUCTURE_ID = "SAMPLE_STRUCTURE_ID";
    private static final Date NEXT_VISIBLE_TIME = new Date(System.currentTimeMillis());
    private static final String MESSAGE_ID = "SAMPLE_MESSAGE_ID";
    private static final Integer DEQUEUE_COUNT = 1;

    @Test
    public void run() throws IOException, ParseException {
        operationInfo = OperationInfo.builder().operationId(OPERATION_ID).correlationId(CORRELATION_ID).eventId(EVENT_ID).structureId
                (STRUCTURE_ID).build();
        deleteMonitoringCloudQueueMessage = DeleteMonitoringCloudQueueMessage.builder().operationInfo(mapper.writeValueAsString(operationInfo))
                .nextVisibleTime(NEXT_VISIBLE_TIME.getTime()).build();
        deleteTimeSeriesMonitor.run(mapper.writeValueAsString(operationInfo), NEXT_VISIBLE_TIME, MESSAGE_ID, DEQUEUE_COUNT, InvocationContextTestUtil
                .getMockContext());
        verify(deleteMonitoringProcessor, times(1)).apply(deleteMonitoringCloudQueueMessage);
    }

    @Test
    public void testRunWithException() throws IOException, ParseException {
        operationInfo = OperationInfo.builder().operationId(OPERATION_ID).correlationId(CORRELATION_ID).eventId(EVENT_ID).structureId
                (STRUCTURE_ID).build();
        deleteMonitoringCloudQueueMessage = DeleteMonitoringCloudQueueMessage.builder().operationInfo(mapper.writeValueAsString(operationInfo))
                .nextVisibleTime(NEXT_VISIBLE_TIME.getTime()).build();
        doThrow(new RuntimeException("test error")).when(deleteMonitoringProcessor).apply(deleteMonitoringCloudQueueMessage);
        expectedException.expect(RuntimeException.class);
        deleteTimeSeriesMonitor.run(mapper.writeValueAsString(operationInfo), NEXT_VISIBLE_TIME, MESSAGE_ID, DEQUEUE_COUNT, InvocationContextTestUtil.getMockContext());
    }
}