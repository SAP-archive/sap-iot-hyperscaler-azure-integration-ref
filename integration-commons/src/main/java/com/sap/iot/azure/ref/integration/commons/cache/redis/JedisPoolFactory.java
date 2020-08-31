package com.sap.iot.azure.ref.integration.commons.cache.redis;

import com.sap.iot.azure.ref.integration.commons.cache.CacheConstants;
import com.sap.iot.azure.ref.integration.commons.metrics.MetricsClient;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

class JedisPoolFactory {
    private static final String CACHE_HOST = System.getenv(CacheConstants.AZURE_CACHE_HOST_PROP);
    private static final String CACHE_KEY = System.getenv(CacheConstants.AZURE_CACHE_KEY_PROP);
    private static JedisPool jedisPool;

    static  {
        MetricsClient.timed(() -> {
            jedisPool = new JedisPool(new JedisPoolConfig(), CACHE_HOST, CacheConstants.AZURE_CACHE_PORT, CacheConstants.AZURE_CACHE_TIMEOUT,
                    CACHE_KEY, true);
        }, "RedisCacheInit");
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> jedisPool.close()));
    }

    /**
     * Returns a {@link JedisPool}.
     * Will always return the same instance of the {@link JedisPool}.
     * A shutdown hook is attached so the {@link JedisPool} client will be closed.
     *
     * @return {@link JedisPool} client for the configured cache resource
     */
    synchronized JedisPool getJedisPool() {
        return jedisPool;
    }
}
