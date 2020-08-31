package com.sap.iot.azure.ref.integration.commons.cache.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.cache.CacheConstants;
import com.sap.iot.azure.ref.integration.commons.cache.api.CacheRepository;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;

public class AzureCacheRepository implements CacheRepository {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final JedisPool jedisPool;

    public AzureCacheRepository() {
        this(new JedisPoolFactory().getJedisPool());
    }

    AzureCacheRepository(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * Gets a cache entry from the configured Azure Redis Cache resource.
     * Uses the {@link ObjectMapper} to parse the cache entry to an instance of the provided class.
     *
     * @param key,   of the cache entry
     * @param clazz, class which the cache entry is parsed to
     * @return {@link Optional} which contains the cache entry. Is empty if no cache entry is found.
     * @throws IoTRuntimeException of type {@link com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType#CACHE_ACCESS_ERROR}
     */
    @Override
    public <T> Optional<T> get(byte[] key, Class<T> clazz) throws IoTRuntimeException {
        try (Jedis jedis = jedisPool.getResource()) {
            Optional<T> value = Optional.empty();
            byte[] cacheEntry = jedis.get(key);

            if (cacheEntry != null) {
                value = Optional.of(objectMapper.readValue(cacheEntry, clazz));
            }

            return value;

        } catch (IOException ex) {
            InvocationContext.getLogger().log(Level.WARNING, "Deleting invalid Cache Entry with key: " + getKeyAsString(key), ex);
            delete(key);
            return Optional.empty();
        } catch (JedisException e) {
            throw IoTRuntimeException.wrapTransient(IdentifierUtil.getIdentifier(CommonConstants.CACHE_KEY, getKeyAsString(key)),
                    CommonErrorType.CACHE_ACCESS_ERROR, "Error in reading redis", e);
        }
    }

    /**
     * Set a cache entry for a given key. The value is passed as POJO.
     * The {@link ObjectMapper} is used to parse the POJO to a JSON string and serialize it to a byte array.
     *
     * @param key,   of the cache entry
     * @param t,     cache value
     * @param clazz, class of the cache value
     * @throws IoTRuntimeException of type {@link com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType#CACHE_ACCESS_ERROR}
     */
    @Override
    public <T> void set(byte[] key, T t, Class<T> clazz) throws IoTRuntimeException {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(key, objectMapper.writeValueAsString(t).getBytes(StandardCharsets.UTF_8));

        } catch (JsonProcessingException e) {
            InvocationContext.getLogger().log(Level.SEVERE, "Unable to Parse Object for Cache Key: " + getKeyAsString(key), e);
        } catch (JedisException e) {
            throw IoTRuntimeException.wrapTransient(IdentifierUtil.getIdentifier(CommonConstants.CACHE_KEY, getKeyAsString(key)),
                    CommonErrorType.CACHE_ACCESS_ERROR, "Error in setting cache entry", e);
        }
    }

    /**
     * Deletes the cache entry for the given key.
     *
     * @param key, of the cache entry
     * @throws IoTRuntimeException of type {@link com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType#CACHE_ACCESS_ERROR}
     */
    @Override
    public void delete(byte[] key) throws IoTRuntimeException {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
        } catch (JedisException e) {
            throw IoTRuntimeException.wrapTransient(IdentifierUtil.getIdentifier(CommonConstants.CACHE_KEY, getKeyAsString(key)),
                    CommonErrorType.CACHE_ACCESS_ERROR, "Error in deleting cache entry", e);
        }
    }

    /**
     * Scans the cache entry for the given partialKey using cursor based pattern matching
     *
     * @param partialKey, used for scanning the matching keys
     * @return list {@link List <String>} containing a string of resulting keys retrieved using scan call
     */
    @Override
    public List<String> scanCacheKey(String partialKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> keys = new ArrayList<>();
            String cursor = CacheConstants.SCAN_CURSOR;
            ScanParams sp = new ScanParams();
            sp.match(partialKey + CacheConstants.SCAN_MATCH_ASTERISK);
            sp.count(CacheConstants.SCAN_COUNT);
            do {
                ScanResult<String> ret = jedis.scan(cursor, sp);
                List<String> result = ret.getResult();
                if (result != null && result.size() > 0) {
                    keys.addAll(result);
                }
                cursor = ret.getCursor();
            } while (!cursor.equals(CacheConstants.SCAN_CURSOR));
            return keys;
        }
    }

    @NotNull
    private String getKeyAsString(byte[] key) {
        return new String(key, StandardCharsets.UTF_8);
    }
}