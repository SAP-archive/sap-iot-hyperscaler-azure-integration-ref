package com.sap.iot.azure.ref.integration.commons.cache.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.cache.CacheConstants;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AzureCacheRepositoryTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Mock
    private JedisPool jedisPool;
    @Mock
    private Jedis jedis;

    @Captor
    private ArgumentCaptor<byte[]> keyCaptor;
    @Captor
    private ArgumentCaptor<byte[]> valueCaptor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AzureCacheRepository azureCacheRepository;
    private TestPojo testPojo = TestPojo.builder().name("name").number(123).build();
    private String testKey = "test";
    private byte[] testKeyAsBytes = testKey.getBytes();

    @Before
    public void prepare() {
        doReturn(jedis).when(jedisPool).getResource();
        azureCacheRepository = new AzureCacheRepository(jedisPool);
    }

    @Test
    public void testGet() throws JsonProcessingException {
        doReturn(objectMapper.writeValueAsString(testPojo).getBytes()).when(jedis).get(testKeyAsBytes);

        Optional<TestPojo> cacheEntry = azureCacheRepository.get(testKeyAsBytes, TestPojo.class);
        verify(jedis, times(1)).get(keyCaptor.capture());

        assertEquals(testKeyAsBytes, keyCaptor.getValue());
        assertEquals(true, cacheEntry.isPresent());
        assertEquals(testPojo, cacheEntry.get());
    }

    @Test
    public void testInvalidEntry() {
        doReturn("Invalid".getBytes()).when(jedis).get(testKeyAsBytes);

        Optional<TestPojo> cacheEntry = azureCacheRepository.get(testKeyAsBytes, TestPojo.class);
        verify(jedis, times(1)).get(keyCaptor.capture());
        verify(jedis, times(1)).del(any(byte[].class));

        assertEquals(false, cacheEntry.isPresent());
    }

    @Test
    public void testGetJedisException() {
        doThrow(JedisException.class).when(jedis).get(any(byte[].class));
        expectedException.expect(IoTRuntimeException.class);
        expectedException.expectMessage("Error in reading redis");
        azureCacheRepository.get(testKeyAsBytes, TestPojo.class);
    }

    @Test
    public void testEmptyGet() {
        Optional<TestPojo> cacheEntry = azureCacheRepository.get(testKeyAsBytes, TestPojo.class);
        verify(jedis, times(1)).get(keyCaptor.capture());

        assertEquals(testKeyAsBytes, keyCaptor.getValue());
        assertEquals(false, cacheEntry.isPresent());
    }

    @Test
    public void testSet() throws JsonProcessingException {
        azureCacheRepository.set(testKeyAsBytes, testPojo, TestPojo.class);
        verify(jedis, times(1)).set(keyCaptor.capture(), valueCaptor.capture());

        assertEquals(testKeyAsBytes, keyCaptor.getValue());
        assertEquals(new String(objectMapper.writeValueAsString(testPojo).getBytes()), new String(valueCaptor.getValue()));
    }

    @Test
    public void testSetJedisException() {
        doThrow(JedisException.class).when(jedis).set(any(byte[].class), any(byte[].class));
        expectedException.expect(IoTRuntimeException.class);
        expectedException.expectMessage("Error in setting cache entry");
        azureCacheRepository.set(testKeyAsBytes, testPojo, TestPojo.class);
    }

    @Test
    public void testDelete() {
        azureCacheRepository.delete(testKeyAsBytes);
        verify(jedis, times(1)).del(keyCaptor.capture());

        assertEquals(testKeyAsBytes, keyCaptor.getValue());
    }

    @Test
    public void testDeleteJedisException() {
        doThrow(JedisException.class).when(jedis).del(any(byte[].class));
        expectedException.expect(IoTRuntimeException.class);
        expectedException.expectMessage("Error in deleting cache entry");
        azureCacheRepository.delete(testKeyAsBytes);
    }

    @Test
    public void testScan() {
        ScanResult result = mock(ScanResult.class);
        doReturn(CacheConstants.SCAN_CURSOR).when(result).getCursor();

        doReturn(result).when(jedis).scan(eq(CacheConstants.SCAN_CURSOR), any(ScanParams.class));
        List<String> scanResult = azureCacheRepository.scanCacheKey("testKey");
        Assert.assertEquals(0, scanResult.size());
        verify(jedis, times(1)).scan(eq(CacheConstants.SCAN_CURSOR), any(ScanParams.class));
    }
}