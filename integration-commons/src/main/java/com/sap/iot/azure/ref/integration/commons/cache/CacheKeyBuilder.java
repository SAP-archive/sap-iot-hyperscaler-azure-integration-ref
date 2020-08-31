package com.sap.iot.azure.ref.integration.commons.cache;

import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;

import java.nio.charset.StandardCharsets;

public class CacheKeyBuilder {

    public static byte[] constructSensorInfoKey(String sensorId, String capabilityId) {
        return (MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX + sensorId + MappingServiceConstants.CACHE_KEY_SEPARATOR + capabilityId).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] constructPropertyMappingInfoKey(String mappingId, String structureId, String virtualCapabilityId) {
        String sep = MappingServiceConstants.CACHE_KEY_SEPARATOR;
        return (MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_MAPPING_KEY_PREFIX + mappingId + sep + structureId + sep + virtualCapabilityId).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] constructSensorKey(String sensorId) {
        return (MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX + sensorId).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] constructSchemaInfoKey(String structureId) {
        return (MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_STRUCTURE_KEY_PREFIX + structureId).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getKeyAsBytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

}