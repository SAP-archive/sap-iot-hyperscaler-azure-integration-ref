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
        Object partitionKey = systemProperties.get(CommonConstants.IOT_HUB_DEVICE_ID);

        //if the iot hub device id property is not available, we instead take the partition key
        if (partitionKey == null) {
            partitionKey = systemProperties.get(CommonConstants.PARTITION_KEY);
        }

        return SystemProperties.builder().enqueuedTimeUtc(systemProperties.get(CommonConstants.ENQUEUED_TIME_UTC).toString())
                .partitionKey(String.valueOf(partitionKey))
                .partitionId(String.valueOf(systemProperties.get(CommonConstants.PARTITION_ID)))
                .offset(String.valueOf(systemProperties.get(CommonConstants.OFFSET)))
                .sequenceNumber(String.valueOf(systemProperties.get(CommonConstants.SEQUENCE_NUMBER)))
                .build();
    }

    public static Map<String, Object> selectRelevantKeys(Map<String, Object> systemProperties) {
        Map<String, Object> systemPropertiesMap = new IdentityHashMap<>();
        systemPropertiesMap.put(CommonConstants.ENQUEUED_TIME_UTC,
                String.valueOf(systemProperties.get(CommonConstants.ENQUEUED_TIME_UTC)));
        systemPropertiesMap.put(CommonConstants.PARTITION_ID,
                String.valueOf(systemProperties.get(CommonConstants.PARTITION_ID)));
        systemPropertiesMap.put(CommonConstants.PARTITION_KEY,
                String.valueOf(systemProperties.get(CommonConstants.PARTITION_KEY)));
        systemPropertiesMap.put(CommonConstants.OFFSET,
                String.valueOf(systemProperties.get(CommonConstants.OFFSET)));
        systemPropertiesMap.put(CommonConstants.SEQUENCE_NUMBER,
                String.valueOf(systemProperties.get(CommonConstants.SEQUENCE_NUMBER)));
        return systemPropertiesMap;
    }
}
