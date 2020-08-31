package com.sap.iot.azure.ref.device.management;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.EventHubTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.sap.iot.azure.ref.device.management.eventhandler.DeviceManagementEventHandler;
import com.sap.iot.azure.ref.device.management.model.DeviceInfo;
import com.sap.iot.azure.ref.device.management.model.DeviceManagementStatusInfo;
import com.sap.iot.azure.ref.device.management.model.cloudevents.SAPIoTAbstractionExtension;
import com.sap.iot.azure.ref.device.management.model.cloudevents.SAPIoTCloudEventType;
import com.sap.iot.azure.ref.device.management.output.DeviceManagementStatusWriter;
import com.sap.iot.azure.ref.device.management.util.Constants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import io.cloudevents.Attributes;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static com.sap.iot.azure.ref.device.management.util.Constants.DEVICE_MANAGEMENT_FUNCTION;
import static com.sap.iot.azure.ref.integration.commons.constants.CommonConstants.*;

@SuppressWarnings("unused")
public class DeviceManagementFunction {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final DeviceManagementEventHandler deviceManagementEventHandler;
    private final DeviceManagementStatusWriter deviceManagementStatusWriter;

    public DeviceManagementFunction() {
        this( new DeviceManagementEventHandler(), new DeviceManagementStatusWriter());
    }

    static {
        InvocationContext.setupInitializationContext(DEVICE_MANAGEMENT_FUNCTION);
    }

    DeviceManagementFunction( DeviceManagementEventHandler deviceManagementEventHandler, DeviceManagementStatusWriter deviceManagementStatusWriter ) {
        this.deviceManagementEventHandler = deviceManagementEventHandler;
        this.deviceManagementStatusWriter = deviceManagementStatusWriter;

        InvocationContext.closeInitializationContext();
    }

    /**
     * handles create or delete of device in IoT hub based on the Device Management message sent from the SAP IoT Abstraction Layer.
     * On completion of the request, the status (either success or failed) is sent back to the SAP IoT Abstraction Layer.
     * @param deviceManagementMessages  incoming device management request messages with device details
     * @param systemProperties          array of message application properties (offset, sequence number, etc)
     * @param executionContext          current method execution context
     */

    @FunctionName(DEVICE_MANAGEMENT_FUNCTION)
    public void run(
            @EventHubTrigger(
                    name = Constants.TRIGGER_NAME,
                    eventHubName = Constants.TRIGGER_EVENT_HUB_NAME,
                    connection = Constants.TRIGGER_EVENT_HUB_CONNECTION_STRING_PROP,
                    consumerGroup = Constants.TRIGGER_EVENT_HUB_CONSUMER_GROUP,
                    dataType = TRIGGER_EVENT_HUB_DATA_TYPE_BINARY,
                    cardinality = Cardinality.MANY
            ) List<byte[]> deviceManagementMessages,
            @BindingName(value = TRIGGER_SYSTEM_PROPERTIES_ARRAY_NAME) Map<String, Object>[] systemProperties,
            @BindingName(value = PARTITION_CONTEXT) Map<String, Object> partitionContext,
            ExecutionContext executionContext) {

        InvocationContext.setupInvocationContext(executionContext);

        try {

            InvocationContext.getLogger().finer(() ->
                    "Incoming device management messages: [" +
                            deviceManagementMessages.stream().map(msg -> new String(msg, StandardCharsets.UTF_8)).collect(Collectors.joining(",")) + "]");

            List<CompletableFuture<Void>> deviceStatusSendFutures = new ArrayList<>();

            for (int i = 0; i < deviceManagementMessages.size(); i++) {
                Map<String, Object> systemProperty = systemProperties[i];
                byte[] deviceManagementMessage = deviceManagementMessages.get(i);

                CloudEvent<Attributes, DeviceInfo> deviceManagementEvent;
                try {
                    deviceManagementEvent = Json.binaryDecodeValue(deviceManagementMessage, CloudEventImpl.class, DeviceInfo.class);
                } catch (IllegalStateException ex) { // exception thrown if conversion to CloudEvent type fails

                    // no status message is written back to SAP IoT since this message is malformed
                    logNonTransientError(String.format("Error in parsing message body %s to type CloudEvent Type", new String(deviceManagementMessage, StandardCharsets.UTF_8)),
                            ex, InvocationContext.getInvocationMessageInfo(partitionContext, systemProperty));

                    continue; // proceed to processing next message in this function
                }

                // handles incoming message and then writes the status back to Status eventhub queue
                deviceStatusSendFutures.add(this.deviceManagementEventHandler.apply(deviceManagementEvent)
                        .thenApplyAsync(deviceManagementStatus -> {

                            // set the source event details to the status message
                            SAPIoTAbstractionExtension sapIoTAbstractionExtension = SAPIoTAbstractionExtension.getExtension(deviceManagementEvent);

                            // add the incoming message transaction id and sequence number in the status sent back
                            return DeviceManagementStatusInfo.builder()
                                    .deviceManagementStatus(deviceManagementStatus)
                                    .sourceEventTransactionId(sapIoTAbstractionExtension.getTransactionId())
                                    .sourceEventSequenceNumber(sapIoTAbstractionExtension.getSequenceNumber())
                                    .sourceEventType(SAPIoTCloudEventType.ofValue(deviceManagementEvent.getAttributes().getType()))
                                    .build();
                        })
                        .thenComposeAsync(deviceManagementStatusWriter)
                        .exceptionally(ex -> {
                            // error in processing single device management request message and will continue to processing other messages in the batch
                            logNonTransientError(String.format("Error in handling device management request with Message Body - %s", Json.encode(deviceManagementEvent)),
                                    ex, InvocationContext.getInvocationMessageInfo(partitionContext, systemProperty));

                            return null;
                        }));
            }

            // wait until all status messages are written to EventHub
            CompletableFuture.allOf(deviceStatusSendFutures.toArray(new CompletableFuture[0])).join();

        } catch (Exception ex) {
            JsonNode batchDetails = InvocationContext.getInvocationBatchInfo(partitionContext, systemProperties);
            InvocationContext.getLogger().log(Level.SEVERE, String.format("Error in sending device management status for batch Partition ID: %s, Start " +
                            "Offset: %s, End Offset: %s", batchDetails.get("PartitionId").asText(), batchDetails.get("OffsetStart").asText(),
                    batchDetails.get("OffsetEnd").asText()), ex);

            // throwing the exception to fail the function execution
            throw ex;
        } finally {
            InvocationContext.closeInvocationContext();
        }
    }

    private void logNonTransientError(String logMessage, Throwable ex, JsonNode identifier) {
        ObjectNode logMessageAsJson = objectMapper.createObjectNode();
        logMessageAsJson.set("Identifier", identifier);
        logMessageAsJson.put("Message", logMessage);
        logMessageAsJson.put("InvocationId", InvocationContext.getContext().getInvocationId());
        logMessageAsJson.put("Transient", false);

        InvocationContext.getLogger().log(Level.SEVERE, logMessageAsJson.toString(), ex);
    }
}