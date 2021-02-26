package com.sap.iot.azure.ref.integration.commons.model.base.eventhub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.IdentityHashMap;
import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class SystemProperties {

    @JsonProperty(CommonConstants.PARTITION_KEY)
    private String partitionKey;

    @JsonProperty(CommonConstants.PARTITION_ID)
    private String partitionId;

    @JsonProperty(CommonConstants.OFFSET)
    private String offset;

    @JsonProperty(CommonConstants.ENQUEUED_TIME_UTC)
    private String enqueuedTimeUtc;

    @JsonProperty(CommonConstants.SEQUENCE_NUMBER)
    private String sequenceNumber;

    public static SystemProperties from(Map<String, Object> systemProperties) {
        return SystemProperties.builder().enqueuedTimeUtc(systemProperties.get(CommonConstants.ENQUEUED_TIME_UTC).toString())
                .partitionKey(systemProperties.get(CommonConstants.PARTITION_KEY).toString())
                .partitionId(systemProperties.get(CommonConstants.PARTITION_ID).toString())
                .offset(systemProperties.get(CommonConstants.OFFSET).toString())
                .sequenceNumber(systemProperties.get(CommonConstants.SEQUENCE_NUMBER).toString()).build();

    }

    public static Map<String, Object> selectRelevantKeys(Map<String, Object> systemProperties) {
        Map<String, Object> systemPropertiesMap = new IdentityHashMap<>();
        systemPropertiesMap.put(CommonConstants.ENQUEUED_TIME_UTC,
                systemProperties.get(CommonConstants.ENQUEUED_TIME_UTC).toString());
        systemPropertiesMap.put(CommonConstants.PARTITION_ID,
                systemProperties.get(CommonConstants.PARTITION_ID).toString());
        systemPropertiesMap.put(CommonConstants.PARTITION_KEY,
                systemProperties.get(CommonConstants.PARTITION_KEY).toString());
        systemPropertiesMap.put(CommonConstants.OFFSET,
                systemProperties.get(CommonConstants.OFFSET).toString());
        systemPropertiesMap.put(CommonConstants.SEQUENCE_NUMBER,
                systemProperties.get(CommonConstants.SEQUENCE_NUMBER).toString());
        return systemPropertiesMap;
    }
}
