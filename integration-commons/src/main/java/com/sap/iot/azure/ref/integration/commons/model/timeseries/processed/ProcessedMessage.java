package com.sap.iot.azure.ref.integration.commons.model.timeseries.processed;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedMessage {

	@NonNull
	private String sourceId;
	private List<Map<String, Object>> measures;
	private Map<String, String> tags;
}
