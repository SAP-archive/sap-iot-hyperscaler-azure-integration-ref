package com.sap.iot.azure.ref.delete;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.QueueTrigger;
import com.sap.iot.azure.ref.delete.exception.DeleteTimeSeriesErrorType;
import com.sap.iot.azure.ref.delete.model.DeleteMonitoringCloudQueueMessage;
import com.sap.iot.azure.ref.delete.storagequeue.DeleteMonitoringProcessor;
import com.sap.iot.azure.ref.delete.util.Constants;
import com.sap.iot.azure.ref.delete.util.HostConfig;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.metrics.MetricsClient;

import java.util.Date;
import java.util.logging.Level;

@SuppressWarnings("unused") // Azure Function
public class DeleteTimeSeriesMonitor {
    private final DeleteMonitoringProcessor deleteMonitoringProcessor;
    private final HostConfig hostConfig;

    public DeleteTimeSeriesMonitor() {
        this(new DeleteMonitoringProcessor(), new HostConfig());
    }

    @VisibleForTesting
    DeleteTimeSeriesMonitor(DeleteMonitoringProcessor deleteMonitoringProcessor, HostConfig hostConfig) {
        this.deleteMonitoringProcessor = deleteMonitoringProcessor;
        this.hostConfig = hostConfig;
    }

    /**
     * Azure Function with Azure Storage Queue trigger that is responsible for monitoring the Delete Operation status. The
     * queue being monitored contains messages with operation id information. The function then handles retrieving operation status
     * and if completed, writes status to an Eventhub. If operation is not completed then the message is re-written into the queue to continue monitoring the
     * operation status.
     *
     * @param message,         incoming delete operation information
     * @param nextVisibleTime, information about the message visibility
     * @param messageId,       message ID used for logging purposes
     * @param dequeueCount,    message dequeue count used for logging purposes
     * @param context,         invocation context of the current Azure Function invocation
     */
    @FunctionName("DeleteMonitoringFunction")
    public void run(
            @QueueTrigger(name = "deleteOperationMonitoringMessage", queueName = Constants.STORAGE_QUEUE_NAME, connection =
                    "operation-storage-connection-string")
                    String message, @BindingName("NextVisibleTime") Date nextVisibleTime, @BindingName("Id") String messageId, @BindingName("DequeueCount")
                    Integer dequeueCount,
            final ExecutionContext context) {

        try {
            DeleteMonitoringCloudQueueMessage deleteMonitoringCloudQueueMessage = DeleteMonitoringCloudQueueMessage.builder().operationInfo(message)
                    .nextVisibleTime(nextVisibleTime.getTime()).build();

            InvocationContext.setupInvocationContext(context);
            InvocationContext.getLogger().log(Level.FINE, String.format("QUEUE_MESSAGE_MONITOR: %s with message id: %s and" +
                    "dequeue count: %s and next visible time: %s", message, messageId, dequeueCount, nextVisibleTime));
            deleteMonitoringProcessor.apply(deleteMonitoringCloudQueueMessage);
        } catch (IoTRuntimeException e) {
            if (e.isTransient()) {
                e.addIdentifier("Message Id", messageId);
                handleRuntimeException(dequeueCount, messageId, e);
            } else {
                InvocationContext.getLogger().log(Level.SEVERE, "Fatal error in processing message in delete monitoring function. The " +
                        "message will not be retried", e);
            }

        } catch (RuntimeException e) {
            handleRuntimeException(dequeueCount, messageId, e);
        } finally {
            InvocationContext.closeInvocationContext();
        }
    }

    private void handleRuntimeException(Integer dequeueCount, String messageId, RuntimeException e) {
        Integer maxDequeueCount = hostConfig.getMaxDequeueCount();
        Integer visibilityTimeout = hostConfig.getVisibilityTimeout();
        if (dequeueCount.equals(maxDequeueCount)) {
            MetricsClient.trackMetric(MetricsClient.getMetricName("PoisonQueueAlert"), 1);
            throw IoTRuntimeException.wrapTransient(IdentifierUtil.getIdentifier("Message Id", messageId),
                    DeleteTimeSeriesErrorType.RUNTIME_ERROR, "Error in processing message in delete monitoring function. The message is" +
                            "now in poison queue since the maximum retries for this message is complete", e);
        } else {
            throw IoTRuntimeException.wrapTransient(IdentifierUtil.getIdentifier("Message Id", messageId),
                    DeleteTimeSeriesErrorType.RUNTIME_ERROR, String.format("Message Id: %s failed to be processed after " +
                            "dequeue count: %s .The message will be retried in: %s seconds", messageId, dequeueCount, visibilityTimeout), e);
        }
    }
}
