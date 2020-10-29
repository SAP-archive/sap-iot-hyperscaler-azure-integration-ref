package com.sap.iot.azure.ref.delete.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.logging.Level;

public class HostConfig {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static JsonNode jsonNode;
//TODO: Add default value
    static{
        try (InputStream is = HostConfig.class.getResourceAsStream("/config/host.json")) {
            jsonNode = mapper.readTree(is);
        } catch (IOException ioException) {
            InvocationContext.getLogger().log(Level.WARNING, "Host config initialization failed", ioException);
            jsonNode = mapper.createObjectNode();
        }
    }

    public Integer getVisibilityTimeout(){
        String visibilityTimeoutStr = jsonNode.get("queues").get("visibilityTimeout").asText();
        Integer visibilityTimeout = Math.toIntExact(Duration.between(LocalTime.MIN, LocalTime.parse(visibilityTimeoutStr)).getSeconds());
        return visibilityTimeout;
    }

    public Integer getMaxDequeueCount(){
        Integer maxDequeueCount = jsonNode.get("queues").get("maxDequeueCount").intValue();
        return maxDequeueCount;
    }
}
