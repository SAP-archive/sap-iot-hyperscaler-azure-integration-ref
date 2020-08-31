package com.sap.iot.azure.ref.ingestion.model.timeseries.raw;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeviceMeasure {
	private String sensorId;
	private String capabilityId;

	@JsonProperty(CommonConstants.TIMESTAMP_PROPERTY_KEY)
	private Instant timestamp;

	private Map<String, Object> properties= new HashMap<String, Object>();

	public Map<String, Object> getProperties() {
		return properties;
	}

	@JsonAnySetter
	@SuppressWarnings("unused") // Used by Jackson
	public void setProperties(String name, Object value) {
		properties.put(name, value);
	}

	public DeviceMeasureKey getGroupingKey() {
		return new DeviceMeasureKey(getSensorId(), getCapabilityId());
	}
}
