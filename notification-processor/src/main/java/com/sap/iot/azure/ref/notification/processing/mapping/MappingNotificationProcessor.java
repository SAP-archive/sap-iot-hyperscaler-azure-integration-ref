package com.sap.iot.azure.ref.notification.processing.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.api.Processor;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.cache.redis.AzureCacheRepository;
import com.sap.iot.azure.ref.integration.commons.cache.CacheKeyBuilder;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
import com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMapping;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMappingInfo;
import com.sap.iot.azure.ref.notification.exception.NotificationErrorType;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.processing.NotificationMessage;
import com.sap.iot.azure.ref.notification.processing.NotificationProcessor;
import com.sap.iot.azure.ref.notification.processing.model.ChangeEntity;
import com.sap.iot.azure.ref.notification.processing.model.ChangeEntityOperation;
import com.sap.iot.azure.ref.notification.processing.model.DataEntity;
import com.sap.iot.azure.ref.notification.processing.model.EntityType;
import com.sap.iot.azure.ref.notification.processing.model.NotificationPropertyMapping;
import com.sap.iot.azure.ref.notification.processing.model.PropertyMappingOperation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;

public class MappingNotificationProcessor implements NotificationProcessor {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final CacheRepository cacheRepository;

    public MappingNotificationProcessor() {
        this(new AzureCacheRepository());
    }

    MappingNotificationProcessor(CacheRepository cacheRepository) {
        this.cacheRepository = cacheRepository;
    }

    /**
     * handleCreate creates a new cache key using the MappingId(changeEntity), virtual capability ID (entityDataList -> providerEntity),
     * structure ID (entityDataList -> entity) and updates value by adding the measure mapping available in the additionalEntity data.
     *
     * @param notification required for fetching information related to the mapping notification parameters.
     */

    @Override
    public void handleCreate(NotificationMessage notification) throws NotificationProcessException {
        String mappingId = notification.getChangeEntity();
        SystemProperties systemProperties = notification.getSource();
        List<ChangeEntity> changeEntities = notification.getChangeList();

        for (ChangeEntity changeEntity : changeEntities) {
            if (changeEntity.getType().equals(EntityType.PROVIDERIOTMAPPING)) {
                byte[] cacheKey = CacheKeyBuilder.constructPropertyMappingInfoKey(mappingId, changeEntity.getEntity(), changeEntity.getProviderEntity());
                addMeasureMapping(cacheKey, mappingId, changeEntity, systemProperties);
            }
        }
    }

    /**
     * handleUpdate updates the Property Mapping Info for a given cache key entry depending on the type of update
     * operation the notification was received for.
     *
     * @param notification required for fetching information related to the mapping notification update.
     */
    @Override
    public void handleUpdate(NotificationMessage notification) throws NotificationProcessException {
        String mappingId = notification.getChangeEntity();
        SystemProperties systemProperties = notification.getSource();
        notification.getChangeList().forEach(changeEntity -> {
            updatePropertyMappingInfo(mappingId, changeEntity, systemProperties);
        });
    }

    /**
     * handleDelete deletes the cache key entry for all entries that contain the Mapping Id (changeEntity)
     * the notification was received for.
     * notification received
     *
     * @param notification required for fetching information related to the mapping notification update.
     */
    @Override
    public void handleDelete(NotificationMessage notification) throws NotificationProcessException {
        List<String> cacheKeys = cacheRepository.scanCacheKey(MappingServiceConstants.CACHE_KEY_CREATOR_PREFIX +
                MappingServiceConstants.CACHE_MAPPING_KEY_PREFIX + notification.getChangeEntity());
        for (String cacheKey : cacheKeys) {
            cacheRepository.delete(cacheKey.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void updatePropertyMappingInfo(String mappingId, ChangeEntity changeEntity, SystemProperties systemProperties) {
        byte[] cacheKey = CacheKeyBuilder.constructPropertyMappingInfoKey(mappingId, changeEntity.getEntity(), changeEntity.getProviderEntity());
        ChangeEntityOperation operation = changeEntity.getOperation();
        List<DataEntity> entityDataList = changeEntity.getAdditionalEntityData();

        switch (operation) {
            case ADD:
                addMeasureMapping(cacheKey, mappingId, changeEntity, systemProperties);
                break;
            case DELETE:
                deleteMeasureMapping(cacheKey);
                break;
            case UPDATE:
                updateMeasureMapping(cacheKey, entityDataList, systemProperties);
                break;
            default:
                InvocationContext.getLogger().log(Level.WARNING, String.format("Unexpected change entity operation for message with following system " +
                        "properties: %s", systemProperties));
        }
    }

    private void addMeasureMapping(byte[] cacheKey, String mappingId, ChangeEntity changeEntity, SystemProperties systemProperties) {
        if (cacheRepository.get(cacheKey, PropertyMappingInfo.class).isPresent()) {
            InvocationContext.getLogger().log(Level.WARNING, String.format("Cache entry for measure mapping exists and will be overwritten. Souce message system " +
                    "properties: %s", systemProperties));
        }

        PropertyMappingInfo propertyMappingInfo = new PropertyMappingInfo();
        propertyMappingInfo.setMappingId(mappingId);
        propertyMappingInfo.setStructureId(changeEntity.getEntity());
        propertyMappingInfo.setVirtualCapabilityId(changeEntity.getProviderEntity());
        addPropertyMappings(propertyMappingInfo, changeEntity.getAdditionalEntityData(), systemProperties);

        cacheRepository.set(cacheKey, propertyMappingInfo, PropertyMappingInfo.class);
    }

    private void addPropertyMappings(PropertyMappingInfo propertyMappingInfo, List<DataEntity> entityDataList, SystemProperties systemProperties) {
        List<PropertyMapping> propertyMappings = new ArrayList<>();
        JsonNode systemPropertiesJson = mapper.convertValue(systemProperties, JsonNode.class);
        if (propertyMappingInfo.getPropertyMappings() != null && !propertyMappingInfo.getPropertyMappings().isEmpty())
            propertyMappings = propertyMappingInfo.getPropertyMappings();

        for (DataEntity entityData : entityDataList) {
            String errorMsg = "Unable to parse property mapping info from entity data";
            NotificationPropertyMapping notificationPropertyMapping = getEntityDataAsPOJO(entityData, systemPropertiesJson, errorMsg);
            //For some reason the capitalization is different in the notification payload. Therefore we need this two step conversion
            PropertyMapping propertyMapping = PropertyMapping.builder()
                    .capabilityPropertyId(notificationPropertyMapping.getCapabilityPropertyId())
                    .structurePropertyId(notificationPropertyMapping.getStructurePropertyId()).build();
            propertyMappings.add(propertyMapping);
        }

        propertyMappingInfo.setPropertyMappings(propertyMappings);
    }

    private void deleteMeasureMapping(byte[] cacheKey) {
        cacheRepository.delete(cacheKey);
    }

    private void updateMeasureMapping(byte[] cacheKey, List<DataEntity> entityDataList, SystemProperties systemProperties) {
        JsonNode systemPropertiesJson = mapper.convertValue(systemProperties, JsonNode.class);
        Optional<PropertyMappingInfo> value = cacheRepository.get(cacheKey, PropertyMappingInfo.class);
        if (value.isPresent()) {
            PropertyMappingInfo propertyMappingInfo = value.get();
            for (DataEntity entityData : entityDataList) {

                String errorMsg = "Unable to parse property mapping info from entity data";
                NotificationPropertyMapping notificationPropertyMapping = getEntityDataAsPOJO(entityData, systemPropertiesJson, errorMsg);

                PropertyMappingOperation operation = notificationPropertyMapping.getOperation();
                switch (operation) {
                    case ADD:
                        addMeasurePropertyMapping(notificationPropertyMapping, propertyMappingInfo);
                        break;
                    case DELETE:
                        deleteMeasurePropertyMapping(notificationPropertyMapping, propertyMappingInfo);
                        break;
                    default:
                        InvocationContext.getLogger().log(Level.WARNING, String.format("Unexpected property mapping operation type for %s",
                                systemProperties));
                }
            }
            cacheRepository.set(cacheKey, propertyMappingInfo, PropertyMappingInfo.class);
        } else {
            InvocationContext.getLogger().log(Level.FINER, "Could not find Measure Mapping to update in cache for message with system");
        }
    }

    private NotificationPropertyMapping getEntityDataAsPOJO(DataEntity entityData, JsonNode systemPropertiesJson, String errorMsg) {
        return ((Processor<String, NotificationPropertyMapping>) message -> {
            try {
                return mapper.readValue(message, NotificationPropertyMapping.class);
            } catch (JsonProcessingException e) {
                throw new IoTRuntimeException(errorMsg, CommonErrorType.JSON_PROCESSING_ERROR, InvocationContext.getContext().getInvocationId(),
                        systemPropertiesJson, false);
            }
        }).apply(entityData.getValue());
    }

    private void addMeasurePropertyMapping(NotificationPropertyMapping notificationPropertyMapping, PropertyMappingInfo propertyMappingInfo) {
        List<PropertyMapping> propertyMappings = propertyMappingInfo.getPropertyMappings();
        PropertyMapping propertyMapping = PropertyMapping.builder()
                .capabilityPropertyId(notificationPropertyMapping.getCapabilityPropertyId())
                .structurePropertyId(notificationPropertyMapping.getStructurePropertyId()).build();
        propertyMappings.add(propertyMapping);
        propertyMappingInfo.setPropertyMappings(propertyMappings);
    }

    private void deleteMeasurePropertyMapping(NotificationPropertyMapping notificationPropertyMapping, PropertyMappingInfo propertyMappingInfo) {
        List<PropertyMapping> propertyMappings = propertyMappingInfo.getPropertyMappings();
        for (int i = propertyMappings.size() - 1; i >= 0; i--) {
            if (propertyMappings.get(i).getCapabilityPropertyId().equals(notificationPropertyMapping.getCapabilityPropertyId())
                    && propertyMappings.get(i).getStructurePropertyId().equals(notificationPropertyMapping.getStructurePropertyId())) {
                propertyMappings.remove(i);
            }
        }
        propertyMappingInfo.setPropertyMappings(propertyMappings);
    }
}