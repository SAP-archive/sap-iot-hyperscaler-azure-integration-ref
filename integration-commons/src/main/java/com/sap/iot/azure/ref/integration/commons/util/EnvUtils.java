package com.sap.iot.azure.ref.integration.commons.util;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Optional;

import static java.lang.Boolean.parseBoolean;

public class EnvUtils {
    private EnvUtils() {
    }

    /**
     * Returns the value of a given environment variable as {@link String}.
     * If the environment variable is not set, the default value is returned.
     *
     * @param propName name of the environment variable
     * @param defaultVal default value
     * @return environment variable as {@link String}
     */
    public static String getEnv(String propName, String defaultVal) {
        return Optional.ofNullable(System.getenv(propName)).orElse(defaultVal);
    }

    /**
     * Returns the value of a given environment variable as int.
     * If the environment variable is not set, the default value is returned.
     *
     * @param propName name of the environment variable
     * @param defaultVal default value
     * @return environment variable as int
     */
    public static int getEnv(String propName, int defaultVal) {
        return Optional.ofNullable(NumberUtils.createInteger(System.getenv(propName))).orElse(defaultVal);
    }

    /**
     * Returns the value of a given environment variable as {@link Boolean}.
     * If the environment variable is not set, the default value is returned.
     *
     * @param propName name of the environment variable
     * @param defaultVal default value
     * @return environment variable as {@link Boolean}
     */
    public static Boolean getEnv(String propName, Boolean defaultVal) {
        String envValue = System.getenv(propName);
        if (envValue != null) {
            return parseBoolean(System.getenv(propName));
        } else {
            return defaultVal;
        }
    }
}
