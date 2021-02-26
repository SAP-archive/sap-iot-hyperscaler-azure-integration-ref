package com.sap.iot.azure.ref.integration.commons.cache;

import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;

import java.nio.charset.StandardCharsets;

public class CacheKeyBuilder {

    /**
     * Generate key for sensor info cache entry from sensor and capability id.
     *
     * @param sensorId used for constructing key
     * @param capabilityId used for constructing key
     * @return sensor info key as byte array
     */
    public static byte[] constructSensorInfoKey(String sensorId, String capabilityId) {
        return (MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX + sensorId + MappingServiceConstants.CACHE_KEY_SEPARATOR + capabilityId).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generate key for property mapping info cache entry from mapping, structure and virtual capability id.
     *
     * @param mappingId used for constructing key
     * @param structureId used for constructing key
     * @param virtualCapabilityId used for constructing key
     * @return property mapping info key as byte array
     */
    public static byte[] constructPropertyMappingInfoKey(String mappingId, String structureId, String virtualCapabilityId) {
        String sep = MappingServiceConstants.CACHE_KEY_SEPARATOR;
        return (MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_MAPPING_KEY_PREFIX + mappingId + sep + structureId + sep + virtualCapabilityId).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generate key for sensor cache entry from sensor id.
     *
     * @param sensorId used for constructing key
     * @return sensor key as byte array
     */
    public static byte[] constructSensorKey(String sensorId) {
        return (MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX + sensorId).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Generate key for schema info cache entry from structure id.
     *
     * @param structureId used for constructing key
     * @return schema info key as byte array
     */
    public static byte[] constructSchemaInfoKey(String structureId) {
        return (MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_STRUCTURE_KEY_PREFIX + structureId).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Returns a given cache key as byte array.
     *
     * @param key to be converted to byte array
     * @return provided key as byte array
     */
    public static byte[] getKeyAsBytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

}