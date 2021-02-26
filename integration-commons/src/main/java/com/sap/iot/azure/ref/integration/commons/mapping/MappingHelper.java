package com.sap.iot.azure.ref.integration.commons.mapping;

import com.sap.iot.azure.ref.integration.commons.adx.ADXTableManager;
import com.sap.iot.azure.ref.integration.commons.cache.CacheKeyBuilder;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.cache.redis.AzureCacheRepository;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.MappingLookupException;
import com.sap.iot.azure.ref.integration.commons.exception.TokenLookupException;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.model.mapping.SensorMappingInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMappingInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SchemaWithADXStatus;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorAssignment;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorInfo;

import java.util.List;
import java.util.Optional;
import java.util.logging.Level;

public class MappingHelper {
    private final MappingServiceLookup mappingServiceLookup;
    private final CacheRepository cacheRepository;
    private final ADXTableManager adxTableManager;

    public MappingHelper() {
        this(new MappingServiceLookup(), new AzureCacheRepository(), new ADXTableManager());
    }

    public MappingHelper(MappingServiceLookup mappingServiceLookup, CacheRepository cacheRepository, ADXTableManager adxTableManager) {
        this.mappingServiceLookup = mappingServiceLookup;
        this.cacheRepository = cacheRepository;
        this.adxTableManager = adxTableManager;
    }
    /**
     * Returns the mapping information for a given sensor ID and virtual Capability ID.
     * The mapping information is, if possible, looked up from the configured cache resource.
     * If the mapping information is not available in the cache, the mapping information is fetched from the mapping APIs.
     *
     * @param sensorId,            used for fetching mapping information
     * @param virtualCapabilityId, used for fetching mapping information
     * @return {@link SensorMappingInfo} containing all mapping information for the provided sensor ID and virtual capability ID.
     * @throws IoTRuntimeException thrown in case mapping lookup fails.
     */

    public SensorMappingInfo getSensorMapping(String sensorId, String virtualCapabilityId) throws IoTRuntimeException {

        try {
            // Fetch deviceInfo from cache, if it exists
            SensorInfo sensorInfo = fetchSensorInfoFromCache(sensorId, virtualCapabilityId)
                    .orElseGet(() -> {
                        // Fetch sensor and assignment information from cache, if it exists
                        Optional<SensorAssignment> sensorAssignmentInfo = fetchSensorAssignmentInfoFromCache(sensorId);
                        return fetchSensorInfoFromAPI(sensorId, virtualCapabilityId, sensorAssignmentInfo);
                    });

            PropertyMappingInfo propertyMappingInfo = fetchPropertyMappingInfofromCache(sensorInfo.getMappingId(), sensorInfo.getStructureId(),
                    virtualCapabilityId).orElseGet(() -> fetchPropertyMappingInfofromAPI(sensorInfo.getMappingId(), sensorInfo.getStructureId(), virtualCapabilityId));

            String schemaInfo = getSchemaInfo(sensorInfo.getStructureId());

            return SensorMappingInfo.builder()
                    .sourceId(sensorInfo.getSourceId())
                    .structureId(sensorInfo.getStructureId())
                    .tags(sensorInfo.getTags())
                    .propertyMappings(propertyMappingInfo.getPropertyMappings())
                    .schemaInfo(schemaInfo)
                    .build();
        } catch (IoTRuntimeException e) {
            e.addIdentifier(CommonConstants.VIRTUAL_CAPABILITY_ID_PROPERTY_KEY, virtualCapabilityId);
            e.addIdentifier(CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId);

            throw e;
        }
    }

    /**
     * Returns the AVRO schema for a given structure Id.
     * The AVRO schema, if possible, looked up from the configured cache resource.
     * If the AVRO schema is not available in the cache, it is fetched from the mapping APIs.
     * If the the AVRO schema is not found in the cache. The ADX Table creation for the provided structure ID is triggered.
     *
     * @param structureId, structure ID for which the AVRO schema is fetched
     * @return AVRO schema info as String
     * @throws TokenLookupException thrown if the authentication token lookup fails
     * @throws ADXClientException thrown in case the ADX table creation fails
     */
    public String getSchemaInfo(String structureId) throws TokenLookupException, ADXClientException {
        //avro schema with ADX table creation status
        SchemaWithADXStatus schemaInfo = fetchSchemaFromCache(structureId)
                .orElseGet(() -> fetchSchemaInfoFromAPI(structureId));

        if (!schemaInfo.isAdxSync()) { // retry sync to adx based on the latest schema
            try {
                adxTableManager.checkIfExists(schemaInfo.getAvroSchema(), structureId);
                saveSchemaInCache(structureId, schemaInfo.withADXSyncStatus(true));
            } catch (ADXClientException e) {

                InvocationContext.getLogger().warning("ADX Sync failed for the structure id " + structureId);
                // will retry based on retry strategy
                throw e;
            }
        }

        return schemaInfo.getAvroSchema();
    }

    public Optional<SensorAssignment> fetchSensorAssignmentInfoFromCache(String sensorId) {
        final byte[] key = CacheKeyBuilder.constructSensorKey(sensorId);
        return cacheRepository.get(key, SensorAssignment.class);
    }

    private Optional<SensorInfo> fetchSensorInfoFromCache(String deviceId, String virtualCapabilityId) {
        final byte[] key = CacheKeyBuilder.constructSensorInfoKey(deviceId, virtualCapabilityId);
        return cacheRepository.get(key, SensorInfo.class);
    }

    private SensorInfo fetchSensorInfoFromAPI(String sensorId, String virtualCapabilityId, Optional<SensorAssignment> sensorAssignment) throws IoTRuntimeException {
        InvocationContext.getLogger().log(Level.FINE, String.format("Fetching device info for sensor ID '%s' and virtual capability ID '%s' from API.", sensorId, virtualCapabilityId));
        SensorInfo sensorInfo = mappingServiceLookup.getSensorInfo(sensorId, virtualCapabilityId, sensorAssignment);
        //Store deviceInfo in cache
        cacheRepository.set(CacheKeyBuilder.constructSensorInfoKey(sensorId, virtualCapabilityId), sensorInfo, SensorInfo.class);
        return sensorInfo;
    }

    private Optional<PropertyMappingInfo> fetchPropertyMappingInfofromCache(String mappingId, String structureId, String virtualCapabilityId) {
        final byte[] key = CacheKeyBuilder.constructPropertyMappingInfoKey(mappingId, structureId, virtualCapabilityId);

        return cacheRepository.get(key, PropertyMappingInfo.class);
    }

    private PropertyMappingInfo fetchPropertyMappingInfofromAPI(String mappingId, String structureId, String virtualCapabilityId) throws MappingLookupException {
        InvocationContext.getLogger().log(Level.FINE, String.format("Fetching Property Mapping Info for mapping ID '%s', structure ID '%s' and virtual " + "capability ID '%s' from API.", mappingId, structureId, virtualCapabilityId));
        List<PropertyMappingInfo> propertyMappingInfos = mappingServiceLookup.getPropertyMappingInfos(mappingId);

        //getPropertyMappingInfos returns property mapping infos for different Virtual Capability ids. We will cache all, but only return the relevant one.
        for (PropertyMappingInfo propertyMappingInfo : propertyMappingInfos) {
            cacheRepository.set(CacheKeyBuilder.constructPropertyMappingInfoKey(mappingId, structureId, propertyMappingInfo.getVirtualCapabilityId()),
                    propertyMappingInfo,
                    PropertyMappingInfo.class);
        }

        Optional<PropertyMappingInfo> propertyMappingInfoOptional =
                propertyMappingInfos.stream().filter(propertyMappingInfo -> propertyMappingInfo.getVirtualCapabilityId().equals(virtualCapabilityId) && propertyMappingInfo.getStructureId().equals(structureId)).findFirst();

        if (propertyMappingInfoOptional.isPresent()) {
            return propertyMappingInfoOptional.get();
        } else {
            throw new MappingLookupException("Did not find PropertyMappingInfo", IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId), false);
        }
    }

    private Optional<SchemaWithADXStatus> fetchSchemaFromCache(String structureId) {
        final byte[] key = CacheKeyBuilder.constructSchemaInfoKey(structureId);
        return cacheRepository.get(key, SchemaWithADXStatus.class);
    }

    private SchemaWithADXStatus fetchSchemaInfoFromAPI(String structureId) throws IoTRuntimeException {
        InvocationContext.getLogger().log(Level.FINE, String.format("Fetching Schema Info for structure ID '%s' from API.", structureId));
        String avroSchema = mappingServiceLookup.getSchemaInfo(structureId);
        SchemaWithADXStatus schemaWithADXStatus = new SchemaWithADXStatus(avroSchema);
        saveSchemaInCache(structureId, schemaWithADXStatus);

        return schemaWithADXStatus;
    }

    public void saveSchemaInCache(String structureId, SchemaWithADXStatus schemaInfo) {
        cacheRepository.set(CacheKeyBuilder.constructSchemaInfoKey(structureId), schemaInfo, SchemaWithADXStatus.class);
    }
}
