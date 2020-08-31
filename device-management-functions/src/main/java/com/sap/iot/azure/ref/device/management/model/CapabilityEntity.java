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
public class CapabilityEntity {
	private String capabilityId;
	private String operation;
	private List<PropertyInfo> properties;

	@Data
	@Builder
	@AllArgsConstructor
	@NoArgsConstructor
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class PropertyInfo {
		private String name;
		private PropertyType type;
		private String length;
		private Operation operation;
	}
}
