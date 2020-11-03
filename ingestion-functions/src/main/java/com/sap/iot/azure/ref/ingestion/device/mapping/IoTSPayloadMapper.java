package com.sap.iot.azure.ref.ingestion.device.mapping;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.ingestion.exception.IngestionErrorType;
import com.sap.iot.azure.ref.ingestion.exception.IngestionRuntimeException;
import com.sap.iot.azure.ref.ingestion.model.device.mapping.DeviceMessage;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasure;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.iots.IoTSMessage;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.iots.IoTSMessageMeasure;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;

import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.CAPABILITY_ID_PROPERTY_KEY;
import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.SENSOR_ID_PROPERTY_KEY;

public class IoTSPayloadMapper implements DevicePayloadMapper {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Maps a {@link DeviceMessage} in IoTS format to a list of {@link DeviceMeasure DeviceMeasures}.
     * The string payload of the {@link DeviceMessage} is deserialized into a {@link IoTSMessage} using the jackson
     * {@link ObjectMapper}. For every measure of the {@link IoTSMessage}, a {@link DeviceMeasure} is created.
     * Supports only Single and Batched Measure IoT Services Message format
     * Single Measure: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2006a/en-US/548edc49e7d24db29826db346a00bb7a.html
     * Batched Measure: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2006a/en-US/3876e012ea2f49dcbbcb1da19e33a1aa.html
     * Batched Measure Message: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2006a/en-US/ed76402f9f044df6a9d1fd5224380f36.html
     *
     * Note: compressed message formats are NOT supported
     * Compressed Single Measure Format: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2006a/en-US/bb62b6d46ee4473b90473eb4178b7617.html
     * Compressed Batched Measure Format: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2006a/en-US/d20327ae27f94abebbcc04469d3630f0.html
     *
     * @param message, device message with payload in IoTS format
     * @return {@link List} of {@link DeviceMeasure DeviceMeasures}
     */
    @Override
    public List<DeviceMeasure> map(DeviceMessage message) throws IngestionRuntimeException {
        try {
            List<DeviceMeasure> rawMessages = new ArrayList<>();
            List<IoTSMessage> ioTSMessages = convertPayloadToIoTSMessages(message.getPayload());
            ioTSMessages.forEach(ioTSMessage -> {
                String sensorId = message.getDeviceId() + Constants.SEPARATOR + ioTSMessage.getSensorAlternateId();
                String capabilityID = ioTSMessage.getCapabilityAlternateId();

                for (IoTSMessageMeasure measure : ioTSMessage.getMeasures()) {

                    Instant eventTimestamp;

                    try {
                        eventTimestamp = getBusinessTimestamp(measure, message);
                    } catch (IngestionRuntimeException ex) {
                        ex.addIdentifier(SENSOR_ID_PROPERTY_KEY, sensorId);
                        ex.addIdentifier(CAPABILITY_ID_PROPERTY_KEY, capabilityID);
                        throw ex;
                    }

                    DeviceMeasure deviceMeasure = DeviceMeasure.builder()
                            .sensorId(sensorId)
                            .capabilityId(capabilityID)
                            .timestamp(eventTimestamp)
                            .properties(measure.getProperties())
                            .build();
                    rawMessages.add(deviceMeasure);
                }
            });

            return rawMessages;
        } catch (IOException ex) { // always JsonParseException - since the data is not fetched over network
            throw new IngestionRuntimeException("Error in parsing device message to valid IoTS Message format", ex, IngestionErrorType.INVALID_DEVICE_MESSAGE,
                    IdentifierUtil.empty(), false);
        }
    }

    private List<IoTSMessage> convertPayloadToIoTSMessages(String payload) throws IOException {
        List<IoTSMessage> ioTSMessages = new ArrayList<>();
        JsonParser parser = null;
        try {
            parser = mapper.getFactory().createParser(payload);
            if (parser.nextToken() == JsonToken.START_ARRAY) {
                ioTSMessages.addAll(mapper.readValue(parser, new TypeReference<List<IoTSMessage>>() {
                }));
            } else {
                ioTSMessages.add(mapper.readValue(parser, IoTSMessage.class));
            }

            return ioTSMessages;
        }finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    /**
     * get the _time (business timestamp or event timestamp) for the given device measurement. The _time value is derived in following order of hierarchy
     * 1. If _time property is available along with the measured values sent from the device in the JSON message payload
     *    _time passed in the measurement payload can be either in ISO8601 format (e.g., 2020-07-15T12:12:12.121Z) or epoch format (e.g., 1594815132121)
     *    If _time contains an invalid value, the service does not process the JSON message payload received from the device
     *
     * 2. If the _time property is not available along with the measured values sent from the device in the JSON message payload, then iothub-enqueuedtime is
     *    used as _time
     *
     * 3. If both _time and iothub-enqueuedtime are not available, the current time at which the record is being processed as the business timestamp
     *
     * @param measure single measurement sent from device
     * @param message entire device message with headers
     * @return _time as {@link Instant}
     */
    private Instant getBusinessTimestamp(IoTSMessageMeasure measure, DeviceMessage message) {
        String timestamp = Objects.toString(measure.getProperties().get(CommonConstants.TIMESTAMP_PROPERTY_KEY), null);

        if (StringUtils.isEmpty(timestamp)) { // use the enqueued_time as the _time value
            timestamp = message.getEnqueuedTime();
        }

        if (StringUtils.isEmpty(timestamp)) { // use current processing time as the _time value
            timestamp = Instant.now().toString();
        }

        Optional<Long> epochMilli = tryParseLong(timestamp);
        if (epochMilli.isPresent()) {
            return Instant.ofEpochMilli(epochMilli.get());
        } else {
            return tryParseInstant(timestamp);
        }
    }

    private Instant tryParseInstant(String timestamp) {
        try {
            return Instant.parse(timestamp);
        } catch (DateTimeParseException ex) {
            throw new IngestionRuntimeException(String.format("Provided %s cannot be parsed to valid timestamp", timestamp), ex,
                    IngestionErrorType.INVALID_TIMESTAMP, IdentifierUtil.empty(), false);
        }
    }

    private Optional<Long> tryParseLong(String timestamp) {
        try {
            return Optional.of(Long.parseLong(timestamp));
        } catch (NumberFormatException ex) {
            InvocationContext.getLogger().log(Level.FINER, () -> String.format("cannot parse %s to long", timestamp));
            return Optional.empty();
        }
    }
}