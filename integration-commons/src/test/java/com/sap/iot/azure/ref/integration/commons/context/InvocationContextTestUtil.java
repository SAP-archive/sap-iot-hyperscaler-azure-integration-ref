package com.sap.iot.azure.ref.integration.commons.context;

import com.google.common.base.Predicates;
import com.microsoft.azure.functions.ExecutionContext;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.mockito.Mockito.*;

public class InvocationContextTestUtil {

    // mocking to be able to verify calls to logger
    public static final Logger LOGGER = spy(Logger.getLogger("test-logger"));
    private static final List<String> LOGS = new ArrayList<>();

    static {
        Mockito.lenient().doAnswer(args -> {
            LogRecord logRecord = args.getArgument(0);
            LOGS.add(logRecord.getMessage());
            if (logRecord.getThrown() != null) {
                LOGS.add(ExceptionUtils.getStackTrace(logRecord.getThrown()));
            }
            return null;
        }).when(LOGGER).log(any(LogRecord.class));
    }

    public static void initInvocationContext() {
        LOGS.clear();
        InvocationContext.setupInvocationContext(getMockContext());
    }

    public static ExecutionContext getMockContext() {

        return new ExecutionContext(){
            private final String invocationId = UUID.randomUUID().toString();
            @Override
            public Logger getLogger() {
                return LOGGER;
            }

            @Override
            public String getInvocationId() {
                return invocationId;
            }

            @Override
            public String getFunctionName() {
                return "testFunction";
            }
        };
    }

    public static Map<String, Object>[] createSystemPropertiesMap(String partitionKey) {

        HashMap<String, Object> properties = new HashMap<>();
        properties.put("iothub-connection-device-id", "");
        properties.put("PartitionKey", partitionKey);
        properties.put("Offset", "OFFSET_VALUE");
        properties.put("SequenceNumber", "SEQUENCE_NUMBER_VALUE");
        properties.put("EnqueuedTimeUtc", "ENQUEUED_TIME_VALUE");

        return new Map[]{properties};
    }

    public static Map<String, Object>[] createSystemPropertiesMap() {
        return createSystemPropertiesMap("PARTITION_KEY_VALUE");
    }

    public static Map<String, Object> createPartitionContext() {
        return Collections.singletonMap("PartitionId", "0");
    }

    public static Optional<String> filterLog(String logPattern) {
        return LOGS.stream()
                .filter(Predicates.containsPattern(logPattern)::apply)
                .findFirst();
    }
}
