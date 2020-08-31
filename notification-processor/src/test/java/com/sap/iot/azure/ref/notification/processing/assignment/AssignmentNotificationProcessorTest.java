package com.sap.iot.azure.ref.notification.processing.assignment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.cache.CacheKeyBuilder;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorAssignment;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.processing.NotificationMessage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class AssignmentNotificationProcessorTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Mock
    CacheRepository cacheRepository;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    AssignmentNotificationProcessor assignmentNotificationProcessor;
    NotificationMessage notificationMessage;

    public static final String SENSOR_ID = "384109E0F2534A6A382J110/ST_J110";
    public static final String ASSIGNMENT_ID = "bdabe161-cdf7-429e-a9f9-db0cb9c67012";
    public static final String MAPPING_ID = "46da85fa-c19d-4c38-8bb6-f0b6f8c76bbe";
    public static final String OBJECT_ID = "384109E0F2534A6A382J110";
    public static final String PARTITION_KEY = "782-342-6723/com.sap.iot.i4c.Assignment/384109E0F2534A6A382J110/ST_J110";
    public static final String INCORRECT_PARTITION_KEY = "782-342-6723/com.sap.iot.i4c.Structure/384109E0F2534A6A382J110/ST_J110";

    @Before
    public void setup() {
        assignmentNotificationProcessor = new AssignmentNotificationProcessor(cacheRepository);
        List<String> keys = new ArrayList<>();
        keys.add(SENSOR_ID);
        Mockito.when(cacheRepository.scanCacheKey(MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX + SENSOR_ID)).thenReturn(keys);
    }

    @Test
    public void testHandleCreate() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/AssignmentNotificationCreateMessage.json"), StandardCharsets.UTF_8);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationMessage.setPartitionKey(PARTITION_KEY);
        assignmentNotificationProcessor.handleCreate(notificationMessage);
        verify(cacheRepository, times(1)).set(CacheKeyBuilder.constructSensorKey(SENSOR_ID), createSensorAssignment(), SensorAssignment.class);
    }

    @Test
    public void testHandleUpdateAddNewSensor() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/AssignmentNotificationUpdateAddSensorMessage.json"), StandardCharsets.UTF_8);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationMessage.setPartitionKey(PARTITION_KEY);
        assignmentNotificationProcessor.handleUpdate(notificationMessage);
        verify(cacheRepository, times(1)).set(CacheKeyBuilder.constructSensorKey(SENSOR_ID), createSensorAssignment(), SensorAssignment.class);
    }

    @Test
    public void testHandleUpdateDeleteSensor() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/AssignmentNotificationUpdateDeleteSensorMessage.json"), StandardCharsets.UTF_8);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationMessage.setPartitionKey(PARTITION_KEY);
        assignmentNotificationProcessor.handleUpdate(notificationMessage);
        verify(cacheRepository, times(1)).scanCacheKey(MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX + SENSOR_ID);
    }

    @Test
    public void testHandleDelete() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/AssignmentNotificationDeleteMessage.json"), StandardCharsets.UTF_8);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationMessage.setPartitionKey(PARTITION_KEY);
        assignmentNotificationProcessor.handleDelete(notificationMessage);
        verify(cacheRepository, times(1)).scanCacheKey(MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX + SENSOR_ID);
    }

    @Test
    public void testHandleNotificationMessageWithMissingMappingId() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/AssignmentNotificationMessageWithMissingObjectId.json"), StandardCharsets.UTF_8);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationMessage.setPartitionKey(PARTITION_KEY);
        try {
            assignmentNotificationProcessor.handleCreate(notificationMessage);
            fail("An expected NotificationProcessorException should have been thrown.");
        }
        catch (NotificationProcessException ex) {
            assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IoTRuntimeException.class)) instanceof NotificationProcessException);
        }
    }

    @Test
    public void testHandleNotificationMessageWithMissingObjectId() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/AssignmentNotificationMessageWithMissingMappingId.json"), StandardCharsets.UTF_8);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationMessage.setPartitionKey(PARTITION_KEY);
        try {
            assignmentNotificationProcessor.handleCreate(notificationMessage);
            fail("An expected NotificationProcessorException should have been thrown.");
        }
        catch (NotificationProcessException ex) {
            assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IoTRuntimeException.class)) instanceof NotificationProcessException);
        }
    }

    @Test
    public void testHandleNotificationMessageWithIncorrectPartitionKey() throws IOException {
        String message = IOUtils.toString(this.getClass().getResourceAsStream("/AssignmentNotificationCreateMessage.json"), StandardCharsets.UTF_8);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationMessage.setPartitionKey(INCORRECT_PARTITION_KEY);
        try {
            assignmentNotificationProcessor.handleCreate(notificationMessage);
            fail("An expected NotificationProcessorException should have been thrown.");
        }
        catch (NotificationProcessException ex) {
            assertTrue(ExceptionUtils.getThrowableList(ex).get(ExceptionUtils.indexOfType(ex, IoTRuntimeException.class)) instanceof NotificationProcessException);
        }
    }

    public static SensorAssignment createSensorAssignment() {
        SensorAssignment sensorAssignment = SensorAssignment.builder()
                .sensorId(SENSOR_ID)
                .assignmentId(ASSIGNMENT_ID)
                .mappingId(MAPPING_ID)
                .objectId(OBJECT_ID)
                .build();
        return sensorAssignment;
    }

}