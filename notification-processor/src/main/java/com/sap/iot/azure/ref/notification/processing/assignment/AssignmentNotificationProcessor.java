package com.sap.iot.azure.ref.notification.processing.assignment;

import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.cache.redis.AzureCacheRepository;
import com.sap.iot.azure.ref.integration.commons.cache.CacheKeyBuilder;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
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
        try {
            String mappingId = getMappingIdFromDataEntityList(notification.getEntityDataList());
            String objectId = getObjectIdFromDataEntityList(notification.getEntityDataList());
            String sensorId = getSensorIdFromPartitionKey(notification.getPartitionKey());
            setSensorAssignmentCacheEntry(sensorId, assignmentId, mappingId, objectId);
        }
        catch (NotificationProcessException ex) {
            ex.addIdentifier(CommonConstants.ASSIGNMENT_ID, assignmentId);
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
        String mappingId = getMappingIdFromDataEntityList(notification.getEntityDataList());
        String sensorId = getSensorIdFromPartitionKey(notification.getPartitionKey());
        String assignmentId = notification.getChangeEntity();
        String objectId = getObjectIdFromDataEntityList(notification.getEntityDataList());
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
                    InvocationContext.getContext().getLogger().log(Level.WARNING, String.format("Unsupported Operation Type: %s.", changeEntity.getOperation()));
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
        String partitionKey = notification.getPartitionKey();
        String sensorId = getSensorIdFromPartitionKey(partitionKey);
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

    private String getMappingIdFromDataEntityList(List<DataEntity> dataEntityList) throws NotificationProcessException {
        for (DataEntity entityData : dataEntityList) {
            if (entityData.getName().equals(MAPPING_ID)) {
                return entityData.getValue();
            }
        }

        throw new NotificationProcessException("Unable to retrieve mappingId from entity data",
                NotificationErrorType.NOTIFICATION_PARSER_ERROR, IdentifierUtil.empty(), false);
    }

    private String getObjectIdFromDataEntityList(List<DataEntity> dataEntityList) throws NotificationProcessException {
        for (DataEntity entityData : dataEntityList) {
            if (entityData.getName().equals(OBJECT_ID)) {
                return entityData.getValue();
            }
        }

        throw new NotificationProcessException("Unable to retrieve objectId from entity data",
                NotificationErrorType.NOTIFICATION_PARSER_ERROR, IdentifierUtil.empty(), false);
    }

    private String getSensorIdFromPartitionKey(String partitionKey) {
        if (partitionKey.contains("Assignment/")) {
            String[] partitionKeyArray = partitionKey.split("Assignment/");
            return partitionKeyArray[partitionKeyArray.length - 1];
        }

        throw new NotificationProcessException("Unable to retrieve sensorId from partitionKey",
                NotificationErrorType.NOTIFICATION_PARSER_ERROR, IdentifierUtil.getIdentifier(CommonConstants.PARTITION_KEY, partitionKey), false);
    }

}