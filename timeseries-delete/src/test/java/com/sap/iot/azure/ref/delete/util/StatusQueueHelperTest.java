package com.sap.iot.azure.ref.delete.util;

import com.sap.iot.azure.ref.delete.model.DeleteStatusMessage;
import com.sap.iot.azure.ref.delete.output.DeleteStatusEventHubProcessor;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import io.cloudevents.CloudEvent;
import io.cloudevents.v1.CloudEventBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class StatusQueueHelperTest {

    @Mock
    private DeleteStatusEventHubProcessor deleteStatusEventHubProcessor;
    @InjectMocks
    StatusQueueHelper statusQueueHelper;

    @Captor
    private ArgumentCaptor<DeleteStatusMessage> messageCaptor;

    private String eventId = "eventId";
    private String structureId = "structureId";
    private String correlationId = "correlationId";

    @Test
    public void testSoftDelete() {
        statusQueueHelper.sendFailedDeleteToStatusQueue(getSampleDeleteInfo(), true);

        verify(deleteStatusEventHubProcessor, times(1)).apply(messageCaptor.capture());
        assertEquals(eventId, messageCaptor.getValue().getEventId());
        assertEquals(structureId, messageCaptor.getValue().getStructureId());
        assertEquals(correlationId, messageCaptor.getValue().getCorrelationId());
    }

    @Test
    public void testPurge() {
        statusQueueHelper.sendFailedDeleteToStatusQueue(getSampleDeleteInfo(), false);

        verify(deleteStatusEventHubProcessor, times(1)).apply(messageCaptor.capture());
        assertEquals(eventId, messageCaptor.getValue().getEventId());
        assertEquals(structureId, messageCaptor.getValue().getStructureId());
        assertEquals(correlationId, messageCaptor.getValue().getCorrelationId());
    }

    @Test
    public void testDeleteRequest() {
        statusQueueHelper.sendFailedDeleteToStatusQueue(eventId);

        verify(deleteStatusEventHubProcessor, times(1)).apply(messageCaptor.capture());
        assertEquals(eventId, messageCaptor.getValue().getEventId());
    }

    private DeleteInfo getSampleDeleteInfo() {
        return DeleteInfo.builder()
                .eventId(eventId)
                .structureId(structureId)
                .correlationId(correlationId)
                .build();
    }

    private CloudEvent getSampleDeleteRequest() {
        return CloudEventBuilder.<DeleteInfo>builder()
                .withId(eventId)
                .withType("sampleType")
                .withSource(URI.create("sampleSource"))
                .build();
    }

}