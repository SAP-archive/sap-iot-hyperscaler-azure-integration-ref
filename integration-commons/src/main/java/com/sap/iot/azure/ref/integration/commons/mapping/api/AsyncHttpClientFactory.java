package com.sap.iot.azure.ref.integration.commons.mapping.api;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.mapping.token.MappingResponseFilter;
import com.sap.iot.azure.ref.integration.commons.metrics.MetricsClient;
import org.asynchttpclient.AsyncHttpClient;

import java.io.IOException;
import java.util.logging.Level;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

public class AsyncHttpClientFactory {
    private static AsyncHttpClient asyncHttpClientWitHResponseFilter;
    private static AsyncHttpClient asyncHttpClient;

    /**
     * Create an {@link AsyncHttpClient} with a response filter.
     * In case of an unauthorized response code, the response filter will refresh the JWT token using the configured token endpoint and credentials and retry
     * the request with the new token.
     * Will always return the same instance of the {@link AsyncHttpClient}.
     * A shutdown hook is attached so the {@link AsyncHttpClient} will be closed.
     *
     * @return {@link AsyncHttpClient} - can be used for APIs which require a valid JWT token
     */
    public synchronized AsyncHttpClient getAsyncHttpClientWitHResponseFilter() {

        MetricsClient.timed(() -> {
            InvocationContext.getLogger().log(Level.FINE, "Fetching Async HTTP Client with response filter.");
            if (asyncHttpClientWitHResponseFilter == null) {
                asyncHttpClientWitHResponseFilter = createAsyncHttpClientWitHResponseFilter();
            }
        }, "HttpClientRespFilterInit");

        return asyncHttpClientWitHResponseFilter;
    }

    AsyncHttpClient createAsyncHttpClientWitHResponseFilter() {
        InvocationContext.getLogger().log(Level.FINE, "Creating Async HTTP Client with response filter.");
        return asyncHttpClientWitHResponseFilter = registerClientShutdown(asyncHttpClient(config()
                .addResponseFilter(new MappingResponseFilter())
                .build()));
    }

    /**
     * Create an {@link AsyncHttpClient}.
     * Will always return the same instance of the {@link AsyncHttpClient}.
     * A shutdown hook is attached so the {@link AsyncHttpClient} will be closed.
     *
     * @return {@link AsyncHttpClient}
     */
    public synchronized AsyncHttpClient getAsyncHttpClient() {

        MetricsClient.timed(() -> {
            InvocationContext.getLogger().log(Level.FINE, "Fetching Async HTTP Client.");
            if (asyncHttpClient == null) {
                asyncHttpClient = createAsyncHttpClient();
            }
        }, "HttpClientInit");

        return asyncHttpClient;
    }

    AsyncHttpClient createAsyncHttpClient() {
        InvocationContext.getLogger().log(Level.FINE, "Creating Async HTTP Client.");
        return asyncHttpClient();
    }

    private AsyncHttpClient registerClientShutdown(AsyncHttpClient client) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownClient(client)));
        return client;
    }

    // visible for test
    synchronized void shutdownClient(AsyncHttpClient client) {
        try {
            client.close();
            asyncHttpClientWitHResponseFilter = null;
            asyncHttpClient = null;
        } catch (IOException e) {
            InvocationContext.getLogger().log(Level.WARNING, "Unable to shut down AsyncHttpClient", e);
        }
    }


}