package com.sap.iot.azure.ref.ingestion.device.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sap.iot.azure.ref.ingestion.exception.IngestionRuntimeException;
import com.sap.iot.azure.ref.ingestion.model.device.mapping.DeviceMessage;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasure;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.core.util.ReflectionUtil;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class IoTDeviceModelPayloadMapperTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String AZ_DEVICE_ID = "deviceId";
    private final String SENSOR_ID_KEY = "sensorAlternateId";
    private final String CAPABILITY_ID_KEY = "capabilityAlternateId";
    private final String SENSOR_ALT_ID = "Pump_00554";
    private final String CAPABILITY_ID = "Rotating_Equipment_Measurements";
    private final String SAMPLE_PROPERTY_KEY = "sampleProperty";
    private final String SAMPLE_PROPERTY_VALUE = "samplePropertyValue";
    private final Instant TIMESTAMP = Instant.now();

    private final IoTDeviceModelPayloadMapper iotDeviceModelPayloadMapper = new IoTDeviceModelPayloadMapper();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupClass() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Test
    public void testAbstractRawMessagesWithIsoTimeFormat()  {
        DeviceMeasure deviceMeasure = iotDeviceModelPayloadMapper.process(getSampleIOTDeviceModelMessages(TIMESTAMP.toString(), true)).get(0);
        assertIoTDeviceModelMeasure(deviceMeasure, false);
    }

    @Test
    public void testAbstractRawMessagesWithLongTimeFormat()  {
        DeviceMeasure deviceMeasure = iotDeviceModelPayloadMapper.process(getSampleIOTDeviceModelMessages(String.valueOf(TIMESTAMP), true)).get(0);
        assertIoTDeviceModelMeasure(deviceMeasure, false);
    }

    @Test
    public void testAbstractRawMessageWithEnqueuedTime() {
        DeviceMeasure deviceMeasure = iotDeviceModelPayloadMapper.process(getSampleIOTDeviceModelMessages(TIMESTAMP.toString(), false)).get(0);
        assertIoTDeviceModelMeasure(deviceMeasure, false);
    }

    @Test
    public void testAbstractRawMessageWithCurrentProcessingTime() {
        DeviceMeasure deviceMeasure = iotDeviceModelPayloadMapper.process(getSampleIOTDeviceModelMessages(null, false)).get(0);
        assertIoTDeviceModelMeasure(deviceMeasure, true);
    }

    @Test
    public void testAbstractRawMessageWithInvalidTimeProperty() throws NoSuchFieldException, IllegalAccessException {

        lenient().doAnswer(args -> {
            LogRecord logRecord = args.getArgument(0);
            assertTrue(logRecord.getMessage().contains("INVALID_TIMESTAMP"));
            assertTrue(logRecord.getMessage().contains("Provided invalid-time cannot be parsed to valid timestamp"));
            return null;
        }).when(InvocationContextTestUtil.LOGGER).log(any(LogRecord.class));
        Mockito.reset(InvocationContextTestUtil.LOGGER);
        iotDeviceModelPayloadMapper.process(getSampleIOTDeviceModelMessages("invalid-time", false));
    }

    @Test
    public void testBatchMessagesWithIsoFormat()  {
        //If I call abstractRawMessages with Messages in IOT Device Model Format, they should be correctly abstracted.
        List<DeviceMeasure> deviceMeasures = iotDeviceModelPayloadMapper.process(getBatchMessages(TIMESTAMP.toString(), true));

        deviceMeasures.forEach(deviceMeasure -> {
            assertIoTDeviceModelMeasure(deviceMeasure, true);
        });
    }

    @Test
    public void testBatchMessagesWithLongFormat()  {
        List<DeviceMeasure> deviceMeasures = iotDeviceModelPayloadMapper.process(getBatchMessages(TIMESTAMP.toString(), true));

        deviceMeasures.forEach(deviceMeasure -> {
            assertIoTDeviceModelMeasure(deviceMeasure, true);
        });
    }

    @Test
    public void testFaultyMessage() {
        expectedException.expect(IngestionRuntimeException.class);
        iotDeviceModelPayloadMapper.process(ConversionTestUtil.getFaultyMessage());

        verify(InvocationContextTestUtil.LOGGER, times(1)).log(any(Level.class), anyString(), any(Throwable.class));
    }

    private void assertIoTDeviceModelMeasure(DeviceMeasure deviceMeasure, boolean processingTimestamp) {
        assertEquals(AZ_DEVICE_ID + Constants.SEPARATOR + SENSOR_ALT_ID, deviceMeasure.getSensorId());
        assertEquals(CAPABILITY_ID, deviceMeasure.getCapabilityId());

        if (processingTimestamp) { // measure timestamp is greater than or equal to the provided timestamp
            assertTrue(deviceMeasure.getTimestamp().equals(TIMESTAMP) || deviceMeasure.getTimestamp().isAfter(TIMESTAMP));
        } else {
            assertEquals(TIMESTAMP, deviceMeasure.getTimestamp());
        }

        assertEquals(SAMPLE_PROPERTY_VALUE, deviceMeasure.getProperties().get(SAMPLE_PROPERTY_KEY).toString());
    }

    private DeviceMessage getSampleIOTDeviceModelMessages(String timestamp, boolean _timeProvided) {
        ObjectNode sampleMessage = getSampleMessageObjectNode(timestamp, _timeProvided);

        return DeviceMessage.builder().deviceId(AZ_DEVICE_ID).enqueuedTime(timestamp).payload(sampleMessage.toString()).build();
    }

    private DeviceMessage getBatchMessages(String timestamp, boolean _timeProvided) {
        ArrayNode sampleMessages = objectMapper.createArrayNode();
        sampleMessages.add(getSampleMessageObjectNode(timestamp, _timeProvided));
        sampleMessages.add(getSampleMessageObjectNode(timestamp, _timeProvided));

        return DeviceMessage.builder().deviceId(AZ_DEVICE_ID).payload(sampleMessages.toString()).build();
    }

    private ObjectNode getSampleMessageObjectNode(String timestamp, boolean _timeProvided) {
        ObjectNode sampleMessage = objectMapper.createObjectNode();
        ArrayNode sampleMeasures = objectMapper.createArrayNode();
        ObjectNode sampleMeasure = objectMapper.createObjectNode();

        sampleMessage.put(SENSOR_ID_KEY, SENSOR_ALT_ID);
        sampleMessage.put(CAPABILITY_ID_KEY, CAPABILITY_ID);

        sampleMeasure.put(SAMPLE_PROPERTY_KEY, SAMPLE_PROPERTY_VALUE);

        if (_timeProvided) // add _time property only if timestamp is provided;
            sampleMeasure.put(CommonConstants.TIMESTAMP_PROPERTY_KEY, timestamp);

        sampleMeasures.add(sampleMeasure);
        sampleMessage.set("measures", sampleMeasures);

        return sampleMessage;
    }
}
