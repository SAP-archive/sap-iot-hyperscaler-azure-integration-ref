package com.sap.iot.azure.ref.ingestion.device.mapping;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.ingestion.exception.IngestionErrorType;
import com.sap.iot.azure.ref.ingestion.exception.IngestionRuntimeException;
import com.sap.iot.azure.ref.ingestion.model.device.mapping.DeviceMessage;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasure;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.device.model.IoTDeviceModelMeasure;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.device.model.IoTDeviceModelMessage;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;

import static com.sap.iot.azure.ref.ingestion.util.Constants.IOT_HUB_DEVICE_ID;
import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.SYSTEM_PROPERTIES;

public class IoTDeviceModelPayloadMapper implements DevicePayloadMapper {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Maps a {@link DeviceMessage} in SAP IoT device model format to a list of {@link DeviceMeasure DeviceMeasures}.
     * The string payload of the {@link DeviceMessage} is deserialized into a {@link IoTDeviceModelMessage} using the jackson
     * {@link ObjectMapper}. For every measure of the {@link IoTDeviceModelMessage}, a {@link DeviceMeasure} is created.
     * Supports only Single and Batched Measure IoT Services Message format
     * Single Measure: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2101a/en-US/755de2516dde4fafb446efaaafb2c81a.html#loioea715dc1481b46fda3c107d740be6b74
     * Batched Measure: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2101a/en-US/755de2516dde4fafb446efaaafb2c81a.html#loio8db4d71ceec3473ba912b533d9dfabea
     * Batched Measure Message: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2101a/en-US/755de2516dde4fafb446efaaafb2c81a.html#loiocab49b18d3c04aafb0eeb407d3dfe09e
     *
     * Note: compressed message formats are NOT supported
     * Compressed Single Measure Format: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2101a/en-US/755de2516dde4fafb446efaaafb2c81a.html#loio7ff1cd67dd234a35b73216749f87e7ed
     * Compressed Batched Measure Format: https://help.sap.com/viewer/9133dbb5799740f8b1e8a1c3f0234776/2101a/en-US/755de2516dde4fafb446efaaafb2c81a.html#loio8e628968720b402398703d98780932b9
     *
     * @param message, device message with payload in SAP IoT device model format
     * @return {@link List} of {@link DeviceMeasure DeviceMeasures}
     */
    @Override
    public List<DeviceMeasure> map(DeviceMessage message) throws IngestionRuntimeException {
        SystemProperties systemProperties = message.getSource();
        JsonNode systemPropertiesJson = mapper.convertValue(systemProperties, JsonNode.class);
        try {
            List<DeviceMeasure> rawMessages = new ArrayList<>();
            List<IoTDeviceModelMessage> ioTDeviceModelMessages = convertPayloadToIoTDeviceModelMessages(message.getPayload());
            ioTDeviceModelMessages.forEach(ioTDeviceModelMessage -> {
                String sensorId = message.getDeviceId() + Constants.SEPARATOR + ioTDeviceModelMessage.getSensorAlternateId();
                String capabilityID = ioTDeviceModelMessage.getCapabilityAlternateId();

                for (IoTDeviceModelMeasure measure : ioTDeviceModelMessage.getMeasures()) {

                    Instant eventTimestamp = ((Processor<IoTDeviceModelMeasure, Instant>) measure1 -> {
                        return getBusinessTimestamp(measure, message);
                    }).apply(measure);

                    if (eventTimestamp != null) {
                        DeviceMeasure deviceMeasure = DeviceMeasure.builder()
                                .sensorId(sensorId)
                                .capabilityId(capabilityID)
                                .timestamp(eventTimestamp)
                                .properties(measure.getProperties())
                                .build();
                        rawMessages.add(deviceMeasure);
                    }
                }
            });

            return rawMessages;
        } catch (IOException ex) { // always JsonParseException - since the data is not fetched over network
            throw new IngestionRuntimeException("Error in parsing device message to valid IoT Device Model Message format", ex, IngestionErrorType.INVALID_DEVICE_MESSAGE,
                    IdentifierUtil.getIdentifier(IOT_HUB_DEVICE_ID, message.getDeviceId(), SYSTEM_PROPERTIES, systemPropertiesJson.toString()), false);
        }
    }

    private List<IoTDeviceModelMessage> convertPayloadToIoTDeviceModelMessages(String payload) throws IOException {
        List<IoTDeviceModelMessage> ioTDeviceModelMessages = new ArrayList<>();
        JsonParser parser = null;
        try {
            parser = mapper.getFactory().createParser(payload);
            if (parser.nextToken() == JsonToken.START_ARRAY) {
                ioTDeviceModelMessages.addAll(mapper.readValue(parser, new TypeReference<List<IoTDeviceModelMessage>>() {
                }));
            } else {
                ioTDeviceModelMessages.add(mapper.readValue(parser, IoTDeviceModelMessage.class));
            }

            return ioTDeviceModelMessages;
        }finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    /**
     * get the _time (business timestamp or event timestamp) for the given device measurement. The _time value is derived in following order of hierarchy
     * 1. If _time property is available along with the measured values sent from the device in the JSON message payload
     * _time passed in the measurement payload can be either in ISO8601 format (e.g., 2020-07-15T12:12:12.121Z) or epoch format (e.g., 1594815132121)
     * If _time contains an invalid value, the service does not process the JSON message payload received from the device
     * <p>
     * 2. If the _time property is not available along with the measured values sent from the device in the JSON message payload, then iothub-enqueuedtime is
     * used as _time
     * <p>
     * 3. If both _time and iothub-enqueuedtime are not available, the current time at which the record is being processed as the business timestamp
     *
     * @param measure single measurement sent from device
     * @param message entire device message with headers
     * @return _time as {@link Instant}
     */
    private Instant getBusinessTimestamp(IoTDeviceModelMeasure measure, DeviceMessage message) {
        String timestamp = Objects.toString(measure.getProperties().get(CommonConstants.TIMESTAMP_PROPERTY_KEY), null);

        if (StringUtils.isEmpty(timestamp)) { // use the enqueued_time as the _time value
            timestamp = message.getEnqueuedTime();
        }

        if (StringUtils.isEmpty(timestamp)) { // use current processing time as the _time value
            timestamp = Instant.now().toString();
        }

        Optional<Long> epochMilli = tryParseLong(timestamp, message);
        if (epochMilli.isPresent()) {
            return Instant.ofEpochMilli(epochMilli.get());
        } else {
            return tryParseInstant(timestamp, message);
        }
    }

    private Instant tryParseInstant(String timestamp, DeviceMessage message) {
        SystemProperties systemProperties = message.getSource();
        JsonNode systemPropertiesJson = mapper.convertValue(systemProperties, JsonNode.class);
        try {
            return Instant.parse(timestamp);
        } catch (DateTimeParseException ex) {
            throw new IngestionRuntimeException(String.format("Provided %s cannot be parsed to valid timestamp", timestamp), ex,
                    IngestionErrorType.INVALID_TIMESTAMP, IdentifierUtil.getIdentifier(SYSTEM_PROPERTIES, systemPropertiesJson.toString()), false);
        }
    }

    private Optional<Long> tryParseLong(String timestamp, DeviceMessage message) {
        SystemProperties systemProperties = message.getSource();
        JsonNode systemPropertiesJson = mapper.convertValue(systemProperties, JsonNode.class);
        try {
            return Optional.of(Long.parseLong(timestamp));
        } catch (NumberFormatException ex) {
            InvocationContext.getLogger().log(Level.FINER, () -> String.format("cannot parse %s to long for message with system properties: %s", timestamp,
                    systemPropertiesJson));
            return Optional.empty();
        }
    }
}