package com.sap.iot.azure.ref.integration.commons.cache.api;

import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;

import java.util.List;
import java.util.Optional;

/**
 * Interface of a Cache resource.
 */
public interface CacheRepository {

    /**
     * Get a cache entry for a given key. The cache entry is returned as on Optional containing an instance of the provided class.
     *
     * @param key,   of the cache entry
     * @param clazz, class which the cache entry is parsed to
     * @return {@link Optional} which contains the cache entry. Is empty if no cache entry is found.
     * @throws IoTRuntimeException with type {@link com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType#CACHE_ACCESS_ERROR}
     */
    <T> Optional<T> get(byte[] key, Class<T> clazz) throws IoTRuntimeException;

    /**
     * Set a cache entry for a given key.
     *
     * @param key,   of the cache entry
     * @param t,     cache value
     * @param clazz, class of the cache value
     * @throws IoTRuntimeException with type {@link com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType#CACHE_ACCESS_ERROR}
     */
    <T> void set(byte[] key, T t, Class<T> clazz) throws IoTRuntimeException;

    /**
     * Remove a cache entry for a given key
     *
     * @param key, of the cache entry
     * @throws IoTRuntimeException with type {@link com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType#CACHE_ACCESS_ERROR}
     */
    void delete(byte[] key) throws IoTRuntimeException;

    /**
     * Scan a cache entry for a given partialKey
     *
     * @param partialKey, of the cache entry
     */
    List<String> scanCacheKey(String partialKey);
}