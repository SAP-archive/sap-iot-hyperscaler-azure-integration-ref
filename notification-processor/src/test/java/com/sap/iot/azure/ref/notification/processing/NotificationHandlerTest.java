package com.sap.iot.azure.ref.notification.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class NotificationHandlerTest {

    private NotificationHandler notificationHandler;

    private NotificationMessage notificationMessage;

    private static final ObjectMapper mapper = new ObjectMapper();

    @Mock
    private NotificationProcessor notificationProcessor;

    @Mock
    private NotificationProcessorFactory notificationProcessorFactory;

    @Before
    public void init() {
        notificationHandler = new NotificationHandler();
        notificationHandler.setNotificationProcessorFactory(notificationProcessorFactory);
        notificationMessage = new NotificationMessage();
        Mockito.when(notificationProcessorFactory.getProcessor(Mockito.any())).thenReturn(notificationProcessor);
    }

    @Test
    public void testCreateExecuteNotificationHandler() throws IOException {
        List<String> notificationMessages = new ArrayList<>();
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationCreateMessage.json"), StandardCharsets.UTF_8);
        notificationMessages.add(message);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationHandler.executeNotificationMessage(notificationMessages, InvocationContextTestUtil.createSystemPropertiesMap());
        Mockito.verify(notificationProcessor).handleCreateWithRetry(Mockito.any());
    }

    @Test
    public void testCreateSourceId() throws IOException {
        List<String> notificationMessages = new ArrayList<>();
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/SourceIdNotificationCreateMessage.json"), StandardCharsets.UTF_8);
        notificationMessages.add(message);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationHandler.executeNotificationMessage(notificationMessages, InvocationContextTestUtil.createSystemPropertiesMap());
        //no exception should be thrown.
    }

    @Test
    public void testUpdateExecuteNotificationHandler() throws IOException {
        List<String> notificationMessages = new ArrayList<>();
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationUpdateAddMeasureMessage.json"), StandardCharsets.UTF_8);
        notificationMessages.add(message);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationHandler.executeNotificationMessage(notificationMessages, InvocationContextTestUtil.createSystemPropertiesMap());
        Mockito.verify(notificationProcessor).handleUpdateWithRetry(Mockito.any());
    }

    @Test
    public void testDeleteExecuteNotificationHandler() throws IOException {
        List<String> notificationMessages = new ArrayList<>();
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationDeleteMessage.json"), StandardCharsets.UTF_8);
        notificationMessages.add(message);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationHandler.executeNotificationMessage(notificationMessages, InvocationContextTestUtil.createSystemPropertiesMap());
        Mockito.verify(notificationProcessor).handleDeleteWithRetry(Mockito.any());
    }
}