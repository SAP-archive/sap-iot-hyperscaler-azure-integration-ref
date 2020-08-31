package com.sap.iot.azure.ref.device.management;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.sdk.iot.service.RegistryManager;
import com.microsoft.azure.sdk.iot.service.exceptions.IotHubException;
import com.microsoft.azure.sdk.iot.service.exceptions.IotHubNotFoundException;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static org.junit.Assert.assertNotNull;

/**
 * integration test - generates a message to the device_management.request event hub and verifies device in IoT Hub
 * the test assumes no other producer or consumer is configured request and status queue respectively
 * requires azure function to be running
 *
 * order of tests is with name of methods - hence adding A, B, C to lexicographically sort methods
 * testDeviceManagementACreate, testDeviceManagementBUpdate, testDeviceManagementCDelete
 */
@Ignore("integration test")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DeviceManagementIT {

    private static final ObjectMapper objMapper = new ObjectMapper();
    private static final Logger LOGGER = Logger.getLogger(DeviceManagementIT.class.getName());

    private static ScheduledExecutorService scheduledExecutorService;
    private static ExecutorService eventHubClientConsumer;
    private static EventHubClient ehProducerClient;
    private static EventHubClient ehConsumerClient;
    private static CountDownLatch statusCheckLatch;
    private static RegistryManager iotHubClient;

    private static final String testDeviceId = "d1";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupClass() throws IOException, EventHubException, ExecutionException, InterruptedException {
        scheduledExecutorService = Executors.newScheduledThreadPool(4);
        eventHubClientConsumer = Executors.newSingleThreadExecutor();

        ehProducerClient = EventHubClient.createFromConnectionStringSync(System.getenv("device-management-request-send-connection-string"),
                scheduledExecutorService);
        ehConsumerClient = EventHubClient.createFromConnectionStringSync(System.getenv("device-management-status-listen-connection-string"),
                scheduledExecutorService);
        iotHubClient = RegistryManager.createFromConnectionString(System.getenv("iothub-registry-connection-string"));
        initializeStatusConsumer();
    }

    private static void initializeStatusConsumer() throws ExecutionException, InterruptedException, EventHubException {

        String partitionId = ehConsumerClient.getRuntimeInformation().get().getPartitionIds()[0];

        PartitionReceiver receiver = ehConsumerClient.createEpochReceiverSync(
                "local-it",
                partitionId,
                EventPosition.fromEndOfStream(),
                1
        );

        eventHubClientConsumer.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                Iterable<EventData> events = receiver.receiveSync(100);

                if (events != null) {
                    for (EventData eventData : events) {
                        try {
                            JsonNode statusMessage = objMapper.readTree(eventData.getBytes());
                            LOGGER.info("Received status - " + statusMessage);
                        } finally {
                            statusCheckLatch.countDown();
                        }
                    }
                }
            }

            return null;
        });
    }

    @AfterClass
    public static void tearDown() {
        // closing Event hub clients
        ehProducerClient.close();
        ehConsumerClient.close();
        scheduledExecutorService.shutdown();
        eventHubClientConsumer.shutdown();

        // closing Event hub clients
        iotHubClient.close();
    }

    @Test
    public void testDeviceManagementACreate() throws IOException, EventHubException, InterruptedException, IotHubException {
        JsonNode jsonNode = objMapper.readTree(this.getClass().getResourceAsStream("/device-management-create-payload.json"));
        ehProducerClient.sendSync(EventData.create(objMapper.writeValueAsBytes(jsonNode)), testDeviceId);

        // the async event hub listens to status events and opens it up once a message is available
        statusCheckLatch = new CountDownLatch(1);
        statusCheckLatch.await();

        // ensure that device exists
        assertNotNull(iotHubClient.getDevice(testDeviceId));
    }

    @Test
    public void testDeviceManagementBUpdate() throws IOException, EventHubException, InterruptedException, IotHubException {
        JsonNode jsonNode = objMapper.readTree(this.getClass().getResourceAsStream("/device-management-update-payload.json"));
        ehProducerClient.sendSync(EventData.create(objMapper.writeValueAsBytes(jsonNode)), testDeviceId);

        statusCheckLatch = new CountDownLatch(1);
        statusCheckLatch.await();

        // ensure that device exists - during update if the device still exists
        assertNotNull(iotHubClient.getDevice(testDeviceId));
    }

    @Test
    public void testDeviceManagementCDelete() throws IOException, EventHubException, InterruptedException, IotHubException {
        JsonNode jsonNode = objMapper.readTree(this.getClass().getResourceAsStream("/device-management-delete-payload.json"));
        ehProducerClient.sendSync(EventData.create(objMapper.writeValueAsBytes(jsonNode)), testDeviceId);

        statusCheckLatch = new CountDownLatch(1);
        statusCheckLatch.await();

        // ensure that device doesn't exists
        expectedException.expect(IotHubNotFoundException.class);
        expectedException.expectMessage("ErrorCode:DeviceNotFound;" + testDeviceId);
        iotHubClient.getDevice(testDeviceId);
    }
}
