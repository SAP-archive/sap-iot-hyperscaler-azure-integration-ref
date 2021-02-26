package com.sap.iot.azure.ref.device.management.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SensorEntity {
	private String sensorId;
	private String sensorAlternateId;
	private String operation;
	private List<CapabilityRef> capabilities;

	@Data
	@Builder
	@AllArgsConstructor
	@NoArgsConstructor
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class CapabilityRef {
		private String capabilityId;
		private String operation;
	}
}
