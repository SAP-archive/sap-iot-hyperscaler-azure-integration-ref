package com.sap.iot.azure.ref.integration.commons.util;

import java.util.Optional;

public class EnvUtils {
    private EnvUtils() {}

    public static String getEnv(String propName, String defaultVal) {
        return Optional.ofNullable(System.getenv(propName)).orElse(defaultVal);
    }
}
