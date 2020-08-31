package com.sap.iot.azure.ref.notification.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.processing.assignment.AssignmentNotificationProcessor;
import com.sap.iot.azure.ref.notification.processing.mapping.MappingNotificationProcessor;
import com.sap.iot.azure.ref.notification.processing.model.EntityType;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.instanceOf;


@RunWith(MockitoJUnitRunner.class)
public class NotificationProcessorFactoryTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private EntityType entityType;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @InjectMocks
    private NotificationProcessorFactory notificationProcessorFactory = new NotificationProcessorFactory();

    @Test
    public void getMappingNotificationProcessor() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/MappingNotificationCreateMessage.json"), StandardCharsets.UTF_8);
        NotificationMessage notificationMessage = mapper.readValue(message, NotificationMessage.class);
        entityType = notificationMessage.getType();
        notificationProcessorFactory.getProcessor(entityType);
        Assert.assertThat(notificationProcessorFactory.getProcessor(entityType), instanceOf(MappingNotificationProcessor.class));
    }

    @Test
    public void getAssignmentNotificationProcessor() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/AssignmentNotificationCreateMessage.json"), StandardCharsets.UTF_8);
        NotificationMessage notificationMessage = mapper.readValue(message, NotificationMessage.class);
        entityType = notificationMessage.getType();
        notificationProcessorFactory.getProcessor(entityType);
        Assert.assertThat(notificationProcessorFactory.getProcessor(entityType), instanceOf(AssignmentNotificationProcessor.class));
    }

    @Test
    public void getDefaultNotificationProcessor() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/StructureNotificationMessageWithIncorrectType.json"), StandardCharsets.UTF_8);
        NotificationMessage notificationMessage = mapper.readValue(message, NotificationMessage.class);
        entityType = notificationMessage.getType();
        expectedException.expect(NotificationProcessException.class);
        notificationProcessorFactory.getProcessor(entityType);
    }

}