package com.sap.iot.azure.ref.ingestion.device.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sap.iot.azure.ref.ingestion.exception.IngestionRuntimeException;
import com.sap.iot.azure.ref.ingestion.model.device.mapping.DeviceMessage;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasure;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.logging.Level;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@RunWith(MockitoJUnitRunner.class)
public class IoTSPayloadMapperTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String AZ_DEVICE_ID = "deviceId";
    private final String SENSOR_ID_KEY = "sensorAlternateId";
    private final String CAPABILITY_ID_KEY = "capabilityAlternateId";
    private final String SENSOR_ALT_ID = "Pump_00554";
    private final String CAPABILITY_ID = "Rotating_Equipment_Measurements";
    private final String SAMPLE_PROPERTY_KEY = "sampleProperty";
    private final String SAMPLE_PROPERTY_VALUE = "samplePropertyValue";
    private final Instant TIMESTAMP = Instant.now();

    private final IoTSPayloadMapper iotsPayloadMapper = new IoTSPayloadMapper();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupClass() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Test
    public void testAbstractRawMessagesWithIsoTimeFormat()  {
        DeviceMeasure deviceMeasure = iotsPayloadMapper.process(getSampleIOTSMessages(TIMESTAMP.toString(), true)).get(0);
        assertIoTSDeviceMeasure(deviceMeasure, false);
    }

    @Test
    public void testAbstractRawMessagesWithLongTimeFormat()  {
        DeviceMeasure deviceMeasure = iotsPayloadMapper.process(getSampleIOTSMessages(String.valueOf(TIMESTAMP.toEpochMilli()), true)).get(0);
        assertIoTSDeviceMeasure(deviceMeasure, false);
    }

    @Test
    public void testAbstractRawMessageWithEnqueuedTime() {
        DeviceMeasure deviceMeasure = iotsPayloadMapper.process(getSampleIOTSMessages(TIMESTAMP.toString(), false)).get(0);
        assertIoTSDeviceMeasure(deviceMeasure, false);
    }

    @Test
    public void testAbstractRawMessageWithCurrentProcessingTime() {
        DeviceMeasure deviceMeasure = iotsPayloadMapper.process(getSampleIOTSMessages(null, false)).get(0);
        assertIoTSDeviceMeasure(deviceMeasure, true);
    }

    @Test
    public void testAbstractRawMessageWithInvalidTimeProperty() {
        expectedException.expect(IngestionRuntimeException.class);
        expectedException.expectMessage("Provided invalid-time cannot be parsed to valid timestamp");
        expectedException.expectMessage("INVALID_TIMESTAMP");
        expectedException.expectMessage("\"Identifier\":{\"sensorId\":\"deviceId/Pump_00554\",\"CapabilityId\":\"Rotating_Equipment_Measurements\"}");
        expectedException.expectCause(isA(DateTimeParseException.class));

        iotsPayloadMapper.process(getSampleIOTSMessages("invalid-time", false));
    }

    @Test
    public void testBatchMessagesWithIsoFormat()  {
        //If I call abstractRawMessages with Messages in IOTS Format, they should be correctly abstracted.
        List<DeviceMeasure> deviceMeasures = iotsPayloadMapper.process(getBatchMessages(TIMESTAMP.toString(), true));

        deviceMeasures.forEach(deviceMeasure -> {
            assertIoTSDeviceMeasure(deviceMeasure, true);
        });
    }

    @Test
    public void testBatchMessagesWithLongFormat()  {
        List<DeviceMeasure> deviceMeasures = iotsPayloadMapper.process(getBatchMessages(TIMESTAMP.toString(), true));

        deviceMeasures.forEach(deviceMeasure -> {
            assertIoTSDeviceMeasure(deviceMeasure, true);
        });
    }

    @Test
    public void testFaultyMessage()  {
        expectedException.expect(IngestionRuntimeException.class);
        iotsPayloadMapper.process(ConversionTestUtil.getFaultyMessage());

        verify(InvocationContextTestUtil.LOGGER, times(1)).log(any(Level.class), anyString(), any(Throwable.class));
    }

    private void assertIoTSDeviceMeasure(DeviceMeasure deviceMeasure, boolean processingTimestamp) {
        assertEquals(AZ_DEVICE_ID + Constants.SEPARATOR + SENSOR_ALT_ID, deviceMeasure.getSensorId());
        assertEquals(CAPABILITY_ID, deviceMeasure.getCapabilityId());

        if (processingTimestamp) { // measure timestamp is greater than or equal to the provided timestamp
            assertTrue(deviceMeasure.getTimestamp().equals(TIMESTAMP) || deviceMeasure.getTimestamp().isAfter(TIMESTAMP));
        } else {
            assertEquals(TIMESTAMP, deviceMeasure.getTimestamp());
        }

        assertEquals(SAMPLE_PROPERTY_VALUE, deviceMeasure.getProperties().get(SAMPLE_PROPERTY_KEY).toString());
    }

    private DeviceMessage getSampleIOTSMessages(String timestamp, boolean _timeProvided) {
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