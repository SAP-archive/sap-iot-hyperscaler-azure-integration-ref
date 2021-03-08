package com.sap.iot.azure.ref.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.EventHubTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.sap.iot.azure.ref.ingestion.device.mapping.DevicePayloadMapper;
import com.sap.iot.azure.ref.ingestion.device.mapping.IoTDeviceModelPayloadMapper;
import com.sap.iot.azure.ref.ingestion.exception.IngestionErrorType;
import com.sap.iot.azure.ref.ingestion.exception.IngestionRuntimeException;
import com.sap.iot.azure.ref.ingestion.model.device.mapping.DeviceMessage;
import com.sap.iot.azure.ref.ingestion.model.timeseries.raw.DeviceMeasure;
import com.sap.iot.azure.ref.ingestion.output.ADXEventHubProcessor;
import com.sap.iot.azure.ref.ingestion.output.ProcessedTimeSeriesEventHubProcessor;
import com.sap.iot.azure.ref.ingestion.processing.DeviceToProcessedMessageProcessor;
import com.sap.iot.azure.ref.ingestion.util.Constants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.metrics.MetricsClient;
import com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static com.sap.iot.azure.ref.ingestion.util.Constants.*;
import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.PARTITION_CONTEXT;
import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.TRIGGER_SYSTEM_PROPERTIES_ARRAY_NAME;

@SuppressWarnings("unused") //AZ Function
public class MappingFunction {

    private final ProcessedTimeSeriesEventHubProcessor processedTimeSeriesEventHubProcessor;
    private final ADXEventHubProcessor adxEventHubProcessor;
    private final DeviceToProcessedMessageProcessor deviceToProcessedMessageProcessor;
    private final Long start = System.currentTimeMillis();

    private DevicePayloadMapper devicePayloadMapper;

    private static final RetryTaskExecutor retryTaskExecutor = new RetryTaskExecutor();
    private static final AtomicBoolean newInstance = new AtomicBoolean(true);
    private static final ObjectMapper objMapper = new ObjectMapper();

    static {
        InvocationContext.setupInitializationContext(INGESTION_FUNCTION);
    }

    public MappingFunction() {
        this(
                new ProcessedTimeSeriesEventHubProcessor(),
                new ADXEventHubProcessor(),
                new DeviceToProcessedMessageProcessor()
        );
    }

    MappingFunction(ProcessedTimeSeriesEventHubProcessor processedTimeSeriesEventHubProcessor, ADXEventHubProcessor adxEventHubProcessor,
                    DeviceToProcessedMessageProcessor deviceToProcessedMessageProcessor) {

        if (newInstance.compareAndSet(true, false)) {
            MetricsClient.trackPerfMetric(MetricsClient.getMetricName("NewInstance"), 1);
        }

        this.processedTimeSeriesEventHubProcessor = processedTimeSeriesEventHubProcessor;
        this.adxEventHubProcessor = adxEventHubProcessor;
        this.deviceToProcessedMessageProcessor = deviceToProcessedMessageProcessor;

        // closing the initialization context
        InvocationContext.closeInitializationContext();
    }

    /**
     * Azure function which invoked by an by an Event Hub trigger.
     * The Trigger is connected to the built in Event Hub Endpoint of the respective IoTHub.
     * The supported payload format is {@link Constants#TRANSFORM_TYPE_IOT_DEVICE_MODEL}.
     * The message payloads are brought into a common format using the {@link DevicePayloadMapper}, augmented with mapping information using the
     * {@link DeviceToProcessedMessageProcessor} and sent to the downstream Event Hubs using the {@link ADXEventHubProcessor} and the
     * {@link ProcessedTimeSeriesEventHubProcessor}.
     *
     * @param messages,         incoming device payloads
     * @param systemProperties, system properties including message header information, such as the IoT Hub device ID
     * @param context,          invocation context of the current Azure Function invocation
     */
    @FunctionName(INGESTION_FUNCTION)
    public void run(
            @EventHubTrigger(
                    name = TRIGGER_NAME,
                    eventHubName = TRIGGER_EVENT_HUB_NAME,
                    connection = TRIGGER_IOT_HUB_CONNECTION_STRING_PROP,
                    consumerGroup = TRIGGER_IOT_HUB_CONSUMER_GROUP,
                    cardinality = Cardinality.MANY
            ) List<String> messages,
            @BindingName(value = TRIGGER_SYSTEM_PROPERTIES_ARRAY_NAME) Map<String, Object>[] systemProperties,
            @BindingName(value = PARTITION_CONTEXT) Map<String, Object> partitionContext,
            final ExecutionContext context) {
        JsonNode batchDetails = InvocationContext.getInvocationBatchInfo(partitionContext, systemProperties);
        try {
            InvocationContext.setupInvocationContext(context);
            MetricsClient.trackPerfMetric(MetricsClient.getMetricName("StartUp"), System.currentTimeMillis() - start);
            trackProcessingOffset(partitionContext, systemProperties);
            devicePayloadMapper = getDevicePayloadToRawMessageMapper();
            ADXEventHubProcessor adxEventHubProcessor;

            retryTaskExecutor.executeWithRetry(() -> processMessages(messages, systemProperties), MAX_RETRIES).join();
            InvocationContext.getLogger().log(Level.INFO, "Completed processing messages");

            // this metric is always published since it's used in the default dashboard
            MetricsClient.trackMetric(MetricsClient.getMetricName("MessagesProcessed"), messages.size());

            // calculate per-message latency in azure ingestion
            trackProcessingLatency(systemProperties);
        } catch (IoTRuntimeException e) {
            e.addIdentifiers((ObjectNode) batchDetails);
            throw e;
        } catch (Exception e) {
            throw new IngestionRuntimeException("Ingestion Failure", e, IngestionErrorType.INVALID_PROCESSED_MESSAGE,
                    batchDetails, false);
        } finally {
            MetricsClient.trackPerfMetric(MetricsClient.getMetricName("RunDuration"), System.currentTimeMillis() - start);
            InvocationContext.closeInvocationContext();
        }
    }

    /**
     * all processes in this method happens in a async thread so that any exception can be caught by the catchExceptionally block
     *
     * @param messages         incoming batch of messages
     * @param systemProperties system properties with partition key and other header properties
     * @return completable future for processing the incoming message asynchronously
     */
    private CompletableFuture<Void> processMessages(List<String> messages, Map<String, Object>[] systemProperties) {

        return CompletableFuture.supplyAsync(InvocationContext.withContext((Supplier<Void>) () -> {
            getDeviceMessages(messages, systemProperties)
                    .stream()
                    .map(devicePayloadMapper).filter(Objects::nonNull)
                    .flatMap(List::stream)
                    .collect(Collectors.groupingBy(DeviceMeasure::getGroupingKey))
                    .entrySet().parallelStream() // processing messages grouped by sourceId in parallel
                    .map(deviceToProcessedMessageProcessor).filter(Objects::nonNull)
                    .forEach(messageGroup -> {
                        /*
                         processed time series event hub processor converts the message to avro format, and send to Eventhub asynchronously
                         if conversion to avro fails, then a null value is returned instead of future corresponding to EventHub send Async
                         if null is returned, the data sent is not complying to SAP-defined AVRO schema, and the message will not be sent for ADX persistence
                         */

                        CompletableFuture<Void> avroConversionAndEventHubSendFuture = processedTimeSeriesEventHubProcessor.apply(messageGroup);

                        // assumes to be completed, and is only triggered after successful avro conversion
                        CompletableFuture<Void> adxEventHubSendFuture = CompletableFuture.completedFuture(null);

                        if (avroConversionAndEventHubSendFuture == null) {
                            // failed avro conversion is treated as permanent failure, and is logged to error (AppInsights)
                            avroConversionAndEventHubSendFuture = CompletableFuture.completedFuture(null);
                        } else {
                            // trigger sending message to EventHub topic for ADX consumption
                            adxEventHubSendFuture = adxEventHubProcessor.apply(messageGroup);
                        }

                        final CompletableFuture<Void> avroConversionAndEventHubSendFutureFinal = avroConversionAndEventHubSendFuture;
                        final CompletableFuture<Void> adxEventHubSendFutureFinal = adxEventHubSendFuture;

                        // wait for sending to EventHub to complete
                        MetricsClient.timed(() -> CompletableFuture.allOf(avroConversionAndEventHubSendFutureFinal, adxEventHubSendFutureFinal).join(),
                                "EventHubSendSync");
                    });

            return null;
        }));
    }

    private List<DeviceMessage> getDeviceMessages(List<String> messages, Map<String, Object>[] systemProperties) {

        List<DeviceMessage> deviceMessages = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            deviceMessages.add(DeviceMessage.builder()
                    .deviceId(Objects.toString(systemProperties[i].get(IOT_HUB_DEVICE_ID), null))
                    .enqueuedTime(Objects.toString(systemProperties[i].get(IOT_HUB_ENQUEUED_TIME), null))
                    .source(SystemProperties.from(systemProperties[i]))
                    .payload(messages.get(i))
                    .build());
        }

        return deviceMessages;
    }

    private void trackProcessingLatency(Map<String, Object>[] messageProps) {
        long now = System.currentTimeMillis();
        for (Map<String, Object> messageProp : messageProps) {
            if (!Objects.isNull(messageProp.get(IOT_HUB_ENQUEUED_TIME))) {
                MetricsClient.trackMetric(MetricsClient.getMetricName("Latency"),
                        now - Instant.parse(messageProp.get(IOT_HUB_ENQUEUED_TIME).toString()).toEpochMilli());
            }
        }
    }

    DevicePayloadMapper getDevicePayloadToRawMessageMapper() {
        if (devicePayloadMapper == null) {
            switch (TRANSFORM_DEFAULT_TYPE) {
                case TRANSFORM_TYPE_IOT_DEVICE_MODEL:
                    devicePayloadMapper = new IoTDeviceModelPayloadMapper();
                    break;
                default:
                    InvocationContext.getLogger().info("No payload type configured. Defaulting to SAP IoT device model Format.");
                    devicePayloadMapper = new IoTDeviceModelPayloadMapper();
                    break;
            }
        }

        return devicePayloadMapper;
    }

    private void trackProcessingOffset(Map<String, Object> partitionContext, Map<String, Object>[] systemProperties) {
        if (InvocationContext.getLogger().isLoggable(Level.FINE) && MetricsClient.PERF_METRICS_ENABLED) {
            JsonNode batchDetails = InvocationContext.getInvocationBatchInfo(partitionContext, systemProperties);
            InvocationContext.getLogger().log(Level.FINE, "OFFSET_MONITOR: " + batchDetails.toString());
        }
    }
}
