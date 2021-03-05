package com.sap.iot.azure.ref.notification.processing.assignment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.cache.redis.AzureCacheRepository;
import com.sap.iot.azure.ref.integration.commons.cache.CacheKeyBuilder;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
import com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorAssignment;
import com.sap.iot.azure.ref.notification.exception.NotificationErrorType;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.processing.NotificationMessage;
import com.sap.iot.azure.ref.notification.processing.NotificationProcessor;
import com.sap.iot.azure.ref.notification.processing.model.ChangeEntity;
import com.sap.iot.azure.ref.notification.processing.model.DataEntity;

import java.util.List;
import java.util.logging.Level;

public class AssignmentNotificationProcessor implements NotificationProcessor {

    private final CacheRepository cacheRepository;
    private final ObjectMapper mapper = new ObjectMapper();

    public static final String MAPPING_ID = "MappingId";
    public static final String OBJECT_ID = "ObjectId";

    public AssignmentNotificationProcessor() {
        this(new AzureCacheRepository());
    }

    @VisibleForTesting
    AssignmentNotificationProcessor(CacheRepository cacheRepository) {
        this.cacheRepository = cacheRepository;
    }

    /**
     * Creates a new cache key with SAP_SENSOR_{SensorId}, and a cache value of {@link SensorAssignment}
     * containing Assignment ID (changeEntity), Mapping ID (entityDataList), Object ID (entityDataList) and Sensor ID (partitionKey).
     *
     * @param notification required for fetching information related to a new sensor for an assignment.
     */
    @Override
    public void handleCreate(NotificationMessage notification) throws NotificationProcessException {
        String assignmentId = notification.getChangeEntity();
        SystemProperties systemProperties = notification.getSource();
        try {
            String mappingId = getMappingIdFromDataEntityList(notification);
            String objectId = getObjectIdFromDataEntityList(notification);
            String sensorId = getSensorIdFromPartitionKey(notification);
            setSensorAssignmentCacheEntry(sensorId, assignmentId, mappingId, objectId);
        } catch (NotificationProcessException ex) {
            ObjectNode systemPropertiesJson = mapper.convertValue(systemProperties, ObjectNode.class);
            ex.addIdentifier(CommonConstants.ASSIGNMENT_ID, assignmentId);
            ex.addIdentifiers(systemPropertiesJson);
            throw ex;
        }
    }

    /**
     * Creates or deletes the cache entry depending upon the operation mentioned in changeList. The ADD operation creates a new cache entry
     * with SAP_SENSOR_{SensorId} as cache key and cache value with Assignment ID (changeEntity), Mapping ID (entityDataList),
     * Object ID (entityDataList) and Sensor ID (partitionKey). The DELETE operation scans the cache entries using Sensor ID (partitionKey)
     * and drops all the matching cache-keys from shared cache, if found.
     *
     * @param notification required for fetching information related to creation or deletion of a sensor for an assignment.
     */
    @Override
    public void handleUpdate(NotificationMessage notification) throws NotificationProcessException {
        String mappingId = getMappingIdFromDataEntityList(notification);
        String sensorId = getSensorIdFromPartitionKey(notification);
        String assignmentId = notification.getChangeEntity();
        String objectId = getObjectIdFromDataEntityList(notification);
        SystemProperties systemProperties = notification.getSource();
        List<ChangeEntity> changeList = notification.getChangeList();

        for (ChangeEntity changeEntity : changeList) {
            switch (changeEntity.getOperation()) {
                case ADD:
                    //set sensor cache using sensor and assignment information
                    setSensorAssignmentCacheEntry(sensorId, assignmentId, mappingId, objectId);
                    break;
                case DELETE:
                    //delete from cache
                    List<String> cacheKeys = cacheRepository.scanCacheKey(MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX + sensorId);
                    for (String cacheKey : cacheKeys) {
                        cacheRepository.delete(CacheKeyBuilder.getKeyAsBytes(cacheKey));
                    }
                    break;
                default:
                    InvocationContext.getContext().getLogger().log(Level.WARNING, String.format("Unsupported operation type: %s. for message with " +
                            "system properties : %s", changeEntity.getOperation(), systemProperties));
            }
        }
    }

    /**
     * Deletes the entries by scanning the cache keys using Sensor ID (partitionKey) and drops all the matching records, if found.
     *
     * @param notification required for fetching information related to deletion of all the sensor/s for an assignment.
     */
    @Override
    public void handleDelete(NotificationMessage notification) throws NotificationProcessException {
        String sensorId = getSensorIdFromPartitionKey(notification);
        List<String> cacheKeys = cacheRepository.scanCacheKey(MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX + MappingServiceConstants.CACHE_SENSOR_KEY_PREFIX + sensorId);
        for (String cacheKey : cacheKeys) {
            cacheRepository.delete(CacheKeyBuilder.getKeyAsBytes(cacheKey));
        }
    }

    private void setSensorAssignmentCacheEntry(String sensorId, String assignmentId, String mappingId, String objectId) {
        SensorAssignment sensorAssignment = SensorAssignment.builder()
                .sensorId(sensorId)
                .assignmentId(assignmentId)
                .mappingId(mappingId)
                .objectId(objectId)
                .build();
        byte[] sensorCacheKey = CacheKeyBuilder.constructSensorKey(sensorId);
        cacheRepository.set(sensorCacheKey, sensorAssignment, SensorAssignment.class);
    }

    private String getMappingIdFromDataEntityList(NotificationMessage notificationMessage) throws NotificationProcessException {
        List<DataEntity> dataEntityList = notificationMessage.getEntityDataList();
        SystemProperties systemProperties = notificationMessage.getSource();
        JsonNode systemPropertiesJson = mapper.convertValue(systemProperties, JsonNode.class);

        for (DataEntity entityData : dataEntityList) {
            if (entityData.getName().equals(MAPPING_ID)) {
                return entityData.getValue();
            }
        }
        throw new NotificationProcessException("Unable to retrieve mappingId from entity data",
                NotificationErrorType.NOTIFICATION_PARSER_ERROR, systemPropertiesJson, false);
    }

    private String getObjectIdFromDataEntityList(NotificationMessage notificationMessage) throws NotificationProcessException {
        List<DataEntity> dataEntityList = notificationMessage.getEntityDataList();
        SystemProperties systemProperties = notificationMessage.getSource();
        JsonNode systemPropertiesJson = mapper.convertValue(systemProperties, JsonNode.class);

        for (DataEntity entityData : dataEntityList) {
            if (entityData.getName().equals(OBJECT_ID)) {
                return entityData.getValue();
            }
        }
        throw new NotificationProcessException("Unable to retrieve objectId from entity data",
                NotificationErrorType.NOTIFICATION_PARSER_ERROR, systemPropertiesJson, false);
    }

    private String getSensorIdFromPartitionKey(NotificationMessage notificationMessage) {
        String partitionKey = notificationMessage.getPartitionKey();
        SystemProperties systemProperties = notificationMessage.getSource();

        if (partitionKey.contains("Assignment/")) {
            String[] partitionKeyArray = partitionKey.split("Assignment/");
            return partitionKeyArray[partitionKeyArray.length - 1];
        }
        JsonNode systemPropertiesJson = mapper.convertValue(systemProperties, JsonNode.class);
        throw new NotificationProcessException("Unable to retrieve sensorId from partitionKey",
                NotificationErrorType.NOTIFICATION_PARSER_ERROR, systemPropertiesJson, false);
    }

}