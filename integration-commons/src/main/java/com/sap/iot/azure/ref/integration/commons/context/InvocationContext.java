package com.sap.iot.azure.ref.integration.commons.context;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.functions.ExecutionContext;
import org.apache.logging.log4j.ThreadContext;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.logging.Filter;
import java.util.logging.Logger;

import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.*;

public class InvocationContext {

    private static final ObjectMapper objMapper = new ObjectMapper();

    // this is used when the invocation execution context is not set; e.g., when logging in a functions's constructor
    private static final Logger INIT_LOGGER = Logger.getLogger("InitLogger");

    private static final String UNKNOWN_FUNCTION = "UnknownFunction";

    // context set after the run method in the function in called
    private static ThreadLocal<ExecutionContext> invocationContextThreadLocal = new ThreadLocal<>();

    // context set during the initialization of the function - e.g., used when reporting metrics / logs in the constructor
    private static ThreadLocal<ExecutionContext> initializationContextThreadLocal = ThreadLocal.withInitial(() -> new ExecutionContext() {
        @Override
        public Logger getLogger() {
            return INIT_LOGGER;
        }

        @Override
        public String getInvocationId() {
            return "";
        }

        @Override
        public String getFunctionName() {
            return UNKNOWN_FUNCTION; // not initialized
        }
    });


    /**
     * Initializes the InvocationContext by passing the {@link ExecutionContext} object for the current Azure Function invocation.
     * Provides a central point for accessing the {@link ExecutionContext} and the included {@link Logger}.
     * This method also extends the {@link Logger} with class and method information.
     *
     * @param context, current execution context should not be null
     */
    public static void setupInvocationContext(@Nonnull ExecutionContext context) {
        InvocationContext.invocationContextThreadLocal.set(context);

        addLoggingFilter(context);
        ThreadContext.put("invocation-id", context.getInvocationId());
    }

    /**
     * Closes all open threadContexts that were used to add invocation id to external libraries using log4j/slf4j
     * Should be invoked before exiting the function invocation.
     */
    public static void closeInvocationContext() {
        // remove the log4j MDC
        ThreadContext.clearAll();

        // remove the execution context - since the same thread can be reused (in case of thread pool
        invocationContextThreadLocal.remove();
    }

    /**
     * sets up the context used within the Function class constructor
     * @param functionName function name
     */
    public static void setupInitializationContext(String functionName) {

        ExecutionContext initializationContext = new ExecutionContext() {
            @Override
            public Logger getLogger() {
                return INIT_LOGGER;
            }

            @Override
            public String getInvocationId() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return functionName;
            }
        };

        initializationContextThreadLocal.set(initializationContext);
        addLoggingFilter(initializationContext);
    }

    public static void closeInitializationContext() {
        initializationContextThreadLocal.remove();
    }

    /**
     * Returns the current {@link ExecutionContext} which was set via the initialize method.
     *
     * @return {@link ExecutionContext}
     */
    public static ExecutionContext getContext() {
        return invocationContextThreadLocal.get() != null ? invocationContextThreadLocal.get() : initializationContextThreadLocal.get() ;
    }

    /**
     * Returns the included {@link Logger} of the {@link ExecutionContext} which was set through the initialize method.
     * If not initialized yet, a {@link Logger} is created and returned.
     *
     * @return {@link ExecutionContext}
     */
    public static Logger getLogger() {
        if (invocationContextThreadLocal.get() == null) {
            return  INIT_LOGGER;  //when Invocation context has not been initialized
        }
        return invocationContextThreadLocal.get().getLogger();
    }

    /**
     * transfer the current ExecutionContext to the new thread for this runnable
     * this util is always used when starting a new thread from the main function execution thread
     * @param runnable task
     * @return runnable with thread context cloned from the thread invoking this function
     */
    public static Runnable withContext(Runnable runnable) {
        ExecutionContext currThreadContext = InvocationContext.getContext();
        return () -> {
            setupInvocationContext(currThreadContext); // set the current thread context to the new Thread that runs this runnable
            runnable.run();
            closeInvocationContext();
        };
    }

    /**
     * transfer the current ExecutionContext to the new thread for this runnable
     * this util is always used when starting a new thread from the main function execution thread
     * @param supplier supplier
     * @return supplier with thread context cloned from the thread invoking this function
     */
    public static <T> Supplier<T> withContext(Supplier<T> supplier) {
        ExecutionContext currThreadContext = InvocationContext.getContext();
        return () -> {
            setupInvocationContext(currThreadContext); // set the current thread context to the new Thread that runs this runnable
            T res = supplier.get();
            closeInvocationContext();
            return res;
        };
    }

    /**
     * transfer the current ExecutionContext to the new thread for this runnable
     * this util is always used when starting a new thread from the main function execution thread
     * @param callable callable
     * @return callable with thread context cloned from the thread invoking this function
     */
    public static <T> Callable<T> withContext(Callable<T> callable) {
        ExecutionContext currThreadContext = InvocationContext.getContext();
        return () -> {
            setupInvocationContext(currThreadContext); // set the current thread context to the new Thread that runs this runnable
            T res = callable.call();
            closeInvocationContext();
            return res;
        };
    }

    /**
     * get the batch details from given {@param partitionContext} and {@param systemProperties}
     * @param partitionContext partition context include partition id
     * @param systemProperties system properties has offset and EnqueuedTime properties for each message in the batch
     * @return JsonNode containing batch information
     */
    public static JsonNode getInvocationBatchInfo(Map<String, Object> partitionContext, Map<String, Object>[] systemProperties) {

        ObjectNode batchInfo = objMapper.createObjectNode();
        batchInfo.put(PARTITION_ID, partitionContext.get(PARTITION_ID).toString());
        batchInfo.put(BATCH_OFFSET_START, systemProperties[0].get(OFFSET).toString());
        batchInfo.put(BATCH_OFFSET_END, systemProperties[systemProperties.length - 1].get(OFFSET).toString());
        batchInfo.put(BATCH_ENQUEUED_TIME_START, systemProperties[0].get(ENQUEUED_TIME_UTC).toString());
        batchInfo.put(BATCH_ENQUEUED_TIME_END, systemProperties[systemProperties.length - 1].get(ENQUEUED_TIME_UTC).toString());
        return batchInfo;
    }

    /**
     * get the message meta info namely partition id, offset and enqueued time for the current function invocation
     * @param partitionContext partition context include partition id
     * @param systemProperty system properties has offset and EnqueuedTime property for the message
     * @return
     */
    public static JsonNode getInvocationMessageInfo(Map<String, Object> partitionContext, Map<String, Object> systemProperty) {
        ObjectNode messageInfo = objMapper.createObjectNode();
        messageInfo.put(PARTITION_ID, partitionContext.get(PARTITION_ID).toString());
        messageInfo.put(OFFSET, systemProperty.get(OFFSET).toString());
        messageInfo.put(ENQUEUED_TIME_UTC, systemProperty.get(ENQUEUED_TIME_UTC).toString());

        return messageInfo;
    }

    private static void addLoggingFilter(@Nonnull ExecutionContext context) {
        Logger logger = context.getLogger();
        Filter enhanceFilter = LoggingMessageFilter.getEnrichedFilter(logger.getFilter());
        logger.setFilter(enhanceFilter);
    }
}