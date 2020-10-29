package com.sap.iot.azure.ref.delete;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.EventHubTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.OFFSET;
import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.PARTITION_ID;

public class DeleteTimeSeriesFunction {

    private final DeleteTimeSeriesHandler deleteTimeSeriesHandler;
    private final RetryTaskExecutor retryTaskExecutor;

    public DeleteTimeSeriesFunction() {
        this(new DeleteTimeSeriesHandler(), new RetryTaskExecutor());
    }

    @VisibleForTesting
    DeleteTimeSeriesFunction(DeleteTimeSeriesHandler deleteTimeSeriesHandler, RetryTaskExecutor retryTaskExecutor) {
        this.deleteTimeSeriesHandler = deleteTimeSeriesHandler;
        this.retryTaskExecutor = retryTaskExecutor;
    }

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

        try {
            InvocationContext.setupInvocationContext(context);
            retryTaskExecutor.executeWithRetry(() -> CompletableFuture.runAsync(InvocationContext.withContext(() ->
                    deleteTimeSeriesHandler.processMessage(message))), Constants.MAX_RETRIES).join();
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
