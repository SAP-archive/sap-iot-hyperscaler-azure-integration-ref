package com.sap.iot.azure.ref.integration.commons.connection;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.metrics.MetricsClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;

public class EventHubClientFactory {
    private static Map<String, CompletableFuture<EventHubClient>> ehClientByConnectionString = new HashMap<>();

    /**
     * Create an {@link EventHubClient} for a given connection string.
     * For every connection string, the same {@link EventHubClient} is always returned.
     * A shutdown hook is attached so the {@link EventHubClient} will be closed.
     *
     * @param connectionString, for which to create an {@link EventHubClient}
     * @return {@link EventHubClient} for given connection string
     */
    public synchronized CompletableFuture<EventHubClient> getEhClient(String connectionString) {
        MetricsClient.timed(() -> {
            InvocationContext.getLogger().log(Level.FINE, "Fetching Event Hub Client.");
            if (ehClientByConnectionString.get(connectionString) == null) {
                ScheduledExecutorService executorService = registerExecutorServiceShutdown(
                        Executors.newScheduledThreadPool(4,
                                new ThreadFactoryBuilder().setNameFormat("event-hub-client-%d").build()));

                EventHubClient ehClient = null;

                try {
                    CompletableFuture<EventHubClient> eventHubClientCompletableFuture = createEventHubClient(connectionString, executorService)
                            .whenCompleteAsync((ehClientAlias, ex) -> registerClientShutdown(ehClient));

                    ehClientByConnectionString.put(connectionString, eventHubClientCompletableFuture);
                } catch (EventHubException | IOException ex) {
                    // shall be retried to create a new connection object todo: to be enhanced with circuit break for stopping ingestion function
                    throw IoTRuntimeException.wrapTransient(IdentifierUtil.empty(), CommonErrorType.EVENT_HUB_ERROR, "Error in creating Event Hub Client",  ex);
                }
            }
        }, "EventHubInit") ;


        return ehClientByConnectionString.get(connectionString);
    }

    /* visible for test */
    CompletableFuture<EventHubClient> createEventHubClient(String connectionString, ScheduledExecutorService executorService) throws IOException, EventHubException {
        InvocationContext.getLogger().log(Level.FINE, "Creating new Event Hub Client.");
        return EventHubClient.createFromConnectionString(connectionString, executorService);
    }

    private ScheduledExecutorService registerExecutorServiceShutdown(ScheduledExecutorService executorService) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownExecutorService(executorService)));

        return executorService;
    }

    // visible for test
    void shutdownExecutorService(ScheduledExecutorService executorService) {
        executorService.shutdown();
    }

    private EventHubClient registerClientShutdown(EventHubClient ehClient) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownClient(ehClient)));
        return ehClient;
    }

    // visible for test
    void shutdownClient(EventHubClient ehClient) {
        try {
            ehClient.closeSync();
            ehClientByConnectionString.clear();
        } catch (EventHubException ex) {
            InvocationContext.getLogger().log(Level.SEVERE, "Unable to close Event Hub connection.", ex);
        }
    }
}
