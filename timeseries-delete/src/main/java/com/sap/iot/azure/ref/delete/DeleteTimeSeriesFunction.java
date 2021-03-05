package com.sap.iot.azure.ref.delete;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.EventHubTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.sap.iot.azure.ref.delete.logic.DeleteTimeSeriesHandler;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.OFFSET;
import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.PARTITION_ID;

public class DeleteTimeSeriesFunction {

    private final DeleteTimeSeriesHandler deleteTimeSeriesHandler;
    private static final RetryTaskExecutor retryTaskExecutor = new RetryTaskExecutor();

    public DeleteTimeSeriesFunction() {
        this(new DeleteTimeSeriesHandler());
    }

    @VisibleForTesting
    DeleteTimeSeriesFunction(DeleteTimeSeriesHandler deleteTimeSeriesHandler) {
        this.deleteTimeSeriesHandler = deleteTimeSeriesHandler;
    }

    /**
     * Azure function which invoked by an by an Event Hub trigger.
     * The Trigger is connected to the Event Hub which is configured through {@link Constants#TRIGGER_EVENT_HUB_CONNECTION_STRING_PROP} environment variable.
     * The message payload is passed to the {@link DeleteTimeSeriesHandler} for further processing. The processing is wrapped with a retry logic implemented
     * by the {@link RetryTaskExecutor}.
     * @param message, incoming delete request
     * @param systemProperties, system properties including message header information
     * @param context, invocation context of the current Azure Function invocation
     */
    @FunctionName("DeleteTimeSeries")
    public void run(
            @EventHubTrigger(
                    name = Constants.TRIGGER_NAME,
                    eventHubName = Constants.TRIGGER_EVENT_HUB_NAME,
                    connection = Constants.TRIGGER_EVENT_HUB_CONNECTION_STRING_PROP,
                    cardinality = Cardinality.ONE,
                    consumerGroup = Constants.TRIGGER_EVENT_HUB_CONSUMER_GROUP
            ) String message,
            @BindingName(value = Constants.TRIGGER_SYSTEM_PROPERTIES_NAME) Map<String, Object> systemProperties,
            @BindingName(value = CommonConstants.PARTITION_CONTEXT) Map<String, Object> partitionContext,
            final ExecutionContext context) {

        Map<String, Object> systemPropertiesMap = SystemProperties.selectRelevantKeys(systemProperties);
        try {
            InvocationContext.setupInvocationContext(context);
            retryTaskExecutor.executeWithRetry(() -> CompletableFuture.runAsync(InvocationContext.withContext(() ->
                    deleteTimeSeriesHandler.processMessage(message, systemPropertiesMap))), Constants.MAX_RETRIES).join();
            InvocationContext.getLogger().log(Level.INFO, " Notification processed");
        } catch (Exception e) {
            JsonNode messageInfo = InvocationContext.getInvocationMessageInfo(partitionContext, systemProperties);
            InvocationContext.getLogger().log(Level.SEVERE, String.format("Delete Time Series failed for message Partition ID: %s, Offset: %s; Exiting after %s",
                    messageInfo.get(PARTITION_ID).asText(), messageInfo.get(OFFSET).asText(), Constants.MAX_RETRIES), e);

            throw e; // ensure function invocation status is set to failed
        } finally {
            InvocationContext.closeInvocationContext();
        }
    }
}
