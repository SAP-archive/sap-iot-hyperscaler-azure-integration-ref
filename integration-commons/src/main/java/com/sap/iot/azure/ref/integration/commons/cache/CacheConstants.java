package com.sap.iot.azure.ref.integration.commons.cache;

public class CacheConstants {

    private CacheConstants() {
    }

    //Azure Cache Constants
    public static final String AZURE_CACHE_HOST_PROP = "azure-cache-host";
    public static final String AZURE_CACHE_KEY_PROP = "azure-cache-key";
    public static final String SCAN_CURSOR = "0";
    public static final String SCAN_MATCH_ASTERISK = "*";
    public static final int SCAN_COUNT = 100;
    public static final int AZURE_CACHE_PORT = 6380;
    public static final int AZURE_CACHE_TIMEOUT = 5000;
}