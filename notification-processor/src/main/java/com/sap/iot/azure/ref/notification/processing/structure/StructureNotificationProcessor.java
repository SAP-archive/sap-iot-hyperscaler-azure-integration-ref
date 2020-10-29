package com.sap.iot.azure.ref.notification.processing.structure;

import com.sap.iot.azure.ref.integration.commons.adx.ADXTableManager;
import com.sap.iot.azure.ref.integration.commons.cache.CacheKeyBuilder;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.cache.redis.AzureCacheRepository;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingHelper;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceLookup;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SchemaWithADXStatus;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;
import com.sap.iot.azure.ref.notification.processing.NotificationMessage;
import com.sap.iot.azure.ref.notification.processing.NotificationProcessor;
import com.sap.iot.azure.ref.notification.processing.model.ChangeEntity;
import com.sap.iot.azure.ref.notification.util.Constants;
import com.sap.iot.azure.ref.notification.util.ModelAbstractionToADXDataTypeMapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.logging.Level;


public class StructureNotificationProcessor implements NotificationProcessor {
    private final MappingServiceLookup mappingServiceLookup;
    private final ADXTableManager adxTableManager;
    private final CacheRepository cacheRepository;
    private final MappingHelper mappingHelper;
    private final RetryTaskExecutor retryTaskExecutor;

    public StructureNotificationProcessor() {
        this(new MappingServiceLookup(), new ADXTableManager(), new AzureCacheRepository());
    }

    StructureNotificationProcessor(MappingServiceLookup mappingServiceLookup, ADXTableManager adxTableManager, CacheRepository cacheRepository) {
        this(mappingServiceLookup, adxTableManager, cacheRepository, new MappingHelper(mappingServiceLookup, cacheRepository, adxTableManager));
    }

    StructureNotificationProcessor(MappingServiceLookup mappingServiceLookup, ADXTableManager adxTableManager, CacheRepository cacheRepository,
                                   MappingHelper mappingHelper) {
        this(mappingServiceLookup, adxTableManager, cacheRepository, mappingHelper, new RetryTaskExecutor());
    }

    StructureNotificationProcessor(MappingServiceLookup mappingServiceLookup, ADXTableManager adxTableManager, CacheRepository cacheRepository,
                                   MappingHelper mappingHelper, RetryTaskExecutor retryTaskExecutor) {
        this.mappingServiceLookup = mappingServiceLookup;
        this.adxTableManager = adxTableManager;
        this.cacheRepository = cacheRepository;
        this.mappingHelper = mappingHelper;
        this.retryTaskExecutor = retryTaskExecutor;
    }

    /**
     * Invokes the handleCreate method, since the retry mechanism is implemented in that method.
     *
     * @param notification notification which is processed
     */
    @Override
    public void handleCreateWithRetry(NotificationMessage notification) {
        handleCreate(notification);
    }

    /**
     * Invokes the handleUpdate method, since the retry mechanism is implemented in that method.
     *
     * @param notification notification which is processed
     */
    @Override
    public void handleUpdateWithRetry(NotificationMessage notification) {
        handleUpdate(notification);
    }

    /**
     * Invokes the handleDelete method, since the retry mechanism is implemented in that method.
     *
     * @param notification notification which is processed
     */
    @Override
    public void handleDeleteWithRetry(NotificationMessage notification) {
        handleDelete(notification);
    }

    /**
     * Picks the structure ID from the notification. With this structure ID, the AVRO schema is fetched from the mapping API.
     * The AVRO schema is stored in the cache and used for creating the ADX table.
     *
     * @param notification notification which is processed
     */
    @Override
    public void handleCreate(NotificationMessage notification) {
        String structureId = getStructureId(notification);

        try {
            //fetch schema with retry
            String schema =
                    retryTaskExecutor.executeWithRetry(() -> CompletableFuture.supplyAsync(InvocationContext.withContext((Supplier<String>) () -> mappingServiceLookup.getSchemaInfo(structureId))),
                            Constants.MAX_RETRIES).join();

            //update cache and ADX with retry
            retryTaskExecutor.executeWithRetry(() -> createCacheAndTable(structureId, schema), Constants.MAX_RETRIES).join();
        } catch (Exception e) {
            InvocationContext.getContext().getLogger().log(Level.WARNING, "Processing of structure create notification failed.");
        }

    }

    /**
     * Picks the structure ID from the notification. With this structure ID, the AVRO schema is fetched from the mapping API.
     * The AVRO schema is stored in the cache, and based on the operation type the ADX table is updated.
     * In case of a delete operation, a data existance check is executed for the affected ADX table column.
     * If no data exists, the column is removed. If data exists, it is instead renamed.
     *
     * @param notification notification which is processed
     */
    @Override
    public void handleUpdate(NotificationMessage notification) {
        List<ChangeEntity> changeList = notification.getChangeList();
        String structureId = getStructureId(notification);

        try {
            //1. fetch schema with retry
            String schema =
                    retryTaskExecutor.executeWithRetry(() -> CompletableFuture.supplyAsync(InvocationContext.withContext((Supplier<String>) () -> mappingServiceLookup.getSchemaInfo(structureId))),
                            Constants.MAX_RETRIES).join();

            //update ADX
            retryTaskExecutor.executeWithRetry(() -> updateCacheAndTable(structureId, schema, changeList), Constants.MAX_RETRIES).join();
        } catch (Exception e) {
            InvocationContext.getContext().getLogger().log(Level.SEVERE, "Processing of structure update notification failed.");
        }
    }

    /**
     * Picks the structure ID from the notification.
     * The AVRO schema is removed in the cache, and the ADX table is removed.
     * If data exists in the ADX table, it is instead renamed.
     *
     * @param notification notification which is processed
     */
    @Override
    public void handleDelete(NotificationMessage notification) {
        String structureId = getStructureId(notification);

        try {
            retryTaskExecutor.executeWithRetry(() -> deleteCacheAndTable(structureId), Constants.MAX_RETRIES).join();
        } catch (Exception e) {
            InvocationContext.getContext().getLogger().log(Level.SEVERE, "Processing of structure delete notification failed.");
        }
    }

    private CompletableFuture<Void> createCacheAndTable(String structureId, String schema) {
        return CompletableFuture.runAsync(InvocationContext.withContext(() -> {
            SchemaWithADXStatus schemaWithADXStatus = new SchemaWithADXStatus(schema);

            mappingHelper.saveSchemaInCache(structureId, schemaWithADXStatus);
            adxTableManager.checkIfExists(schema, structureId);
            mappingHelper.saveSchemaInCache(structureId, schemaWithADXStatus.withADXSyncStatus(true));
        }));
    }

    private CompletableFuture<Void> updateCacheAndTable(String structureId, String schema, List<ChangeEntity> changeList) {
        return CompletableFuture.runAsync(InvocationContext.withContext(() -> {
            //we always want to update the cache
            SchemaWithADXStatus schemaWithADXStatus = new SchemaWithADXStatus(schema);
            mappingHelper.saveSchemaInCache(structureId, schemaWithADXStatus);

            //update ADX
            changeList.forEach(changeEntity -> {
                switch (changeEntity.getOperation()) {
                    case ADD:
                        handlePropertyAdd(structureId, schemaWithADXStatus.getAvroSchema());
                        break;
                    case UPDATE:
                        handlePropertyUpdate(changeEntity, structureId);
                        break;
                    case DELETE:
                        handlePropertyDelete(changeEntity, structureId, schemaWithADXStatus.getAvroSchema());
                        break;
                    default:
                        //for adding or deleting properties, we alter the table with the latest AVRO schema
                        InvocationContext.getContext().getLogger().log(Level.WARNING, String.format("Unsupported Operation Type: %s.",
                                changeEntity.getOperation()));
                }
            });

            mappingHelper.saveSchemaInCache(structureId, schemaWithADXStatus.withADXSyncStatus(true));
        }));
    }

    private void handlePropertyAdd(String structureId, String schema) {
        //update adx table
        adxTableManager.updateTableAndMapping(schema, structureId);
    }

    private void handlePropertyUpdate(ChangeEntity changeEntity, String structureId) {
        String dataType = ModelAbstractionToADXDataTypeMapper.getADXDataType(changeEntity.getAdditionalEntityData().get(0).getValue());

        changeEntity.getAdditionalEntityData().forEach(additionalEntityData -> {
            //we do this one by one to be safe in case of multiple changes
            if (additionalEntityData.getName().equals(Constants.DATA_TYPE_KEY)) {
                adxTableManager.updateColumn(structureId, changeEntity.getEntity(), dataType);
            }
        });
    }

    private void handlePropertyDelete(ChangeEntity changeEntity, String structureId, String schema) {
        String columnName = changeEntity.getEntity();

        if (adxTableManager.dataExistsForColumn(structureId, columnName)) {
            InvocationContext.getContext().getLogger().log(Level.WARNING, String.format("Delete Request received for Column '%s'. Renaming Column to prevent " +
                    "data loss", columnName));
            //rename column
            adxTableManager.softDeleteColumn(structureId, columnName, schema);
        } else {
            // remove column
            adxTableManager.dropColumn(structureId, columnName, schema);
        }
    }

    private CompletableFuture<Void> deleteCacheAndTable(String structureId) {
        return CompletableFuture.runAsync(InvocationContext.withContext(() -> {
            //remove cache entry
            cacheRepository.delete(CacheKeyBuilder.constructSchemaInfoKey(structureId));

            //delete or soft delete ADX table
            if (adxTableManager.dataExists(structureId)) {
                InvocationContext.getContext().getLogger().log(Level.WARNING, String.format("Delete Request received for Structure '%s'. Renaming table to " +
                        "prevent data loss", structureId));
                adxTableManager.softDeleteTable(structureId);
            } else {
                adxTableManager.dropTable(structureId);
            }
        }));
    }


    private String getStructureId(NotificationMessage notification) {
        return notification.getChangeEntity();
    }
}
