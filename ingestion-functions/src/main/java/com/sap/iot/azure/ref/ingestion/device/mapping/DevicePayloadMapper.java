package com.sap.iot.azure.ref.ingestion.device.mapping;

import com.sap.iot.azure.ref.ingestion.exception.IngestionRuntimeException;
import com.sap.iot.azure.ref.ingestion.model.device.mapping.DeviceMessage;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasure;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

import java.util.List;

/**
 * Interface for mapping incoming device messages to a generic format.
 * Classes implementing this interface implement the logic of converting an incoming {@link DeviceMessage} to a list of type {@link DeviceMeasure
 * DeviceMeasures}. the {@link DeviceMessage} holds the Azure device ID as well as the payload as {@link String Strings}.
 * The {@link DeviceMeasure} represents a general format which can be used by other classes of this Azure Function.
 */
public interface DevicePayloadMapper extends Processor<DeviceMessage, List<DeviceMeasure>> {

    /**
     * maps the incoming device message (string) to the {@link DeviceMeasure} identifying sensor & capability id from the device payload
     * @param message incoming device message
     * @return {@link List<DeviceMeasure>} device measures from the given device message
     * @throws IngestionRuntimeException exception in mapping device message to DeviceMeasure
     */
    List<DeviceMeasure> map(DeviceMessage message) throws IngestionRuntimeException;

    /**
     * not expected to be implemented in classes implementing {@link DevicePayloadMapper} interface
     */
    @Override
    default List<DeviceMeasure> process(DeviceMessage deviceMessage) throws IoTRuntimeException {
        return map(deviceMessage);
    }
}
