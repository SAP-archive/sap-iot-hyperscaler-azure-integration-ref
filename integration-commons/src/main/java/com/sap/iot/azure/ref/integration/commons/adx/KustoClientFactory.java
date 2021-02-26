package com.sap.iot.azure.ref.integration.commons.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.metrics.MetricsClient;

import java.net.URISyntaxException;
import java.util.logging.Level;

public class KustoClientFactory {
    private static Client client;
    private static Client ingestionClient;

    /**
     * Returns a Kusto Client by using the connection information from the environment variables {@link ADXConstants#ADX_RESOURCE_URI_PROP},
     * {@link ADXConstants#SERVICE_PRINCIPAL_APPLICATION_CLIENT_ID_PROP}, {@link ADXConstants#SERVICE_PRINCIPAL_APPLICATION_KEY_PROP} and {@link ADXConstants#SERVICE_PRINCIPAL_AUTHORITY_ID_PROP}.
     * The Kusto Client will only be created once.
     *
     * @return {@link Client} Kusto Client for the configured ADX resource
     */
    public static synchronized Client getClient() {
        MetricsClient.timed(() -> {
            InvocationContext.getLogger().log(Level.FINE, "Fetching Kusto Client.");
            if (client == null) {
                client = createClientWithUrl(System.getenv(ADXConstants.ADX_RESOURCE_URI_PROP));
            }
        }, "ADXClientInit");

        return client;
    }

    /**
     * Returns a Kusto Ingestion Client by using the connection information from the environment variables {@link ADXConstants#ADX_INGESTION_URI_PROP},
     * {@link ADXConstants#SERVICE_PRINCIPAL_APPLICATION_CLIENT_ID_PROP}, {@link ADXConstants#SERVICE_PRINCIPAL_APPLICATION_KEY_PROP} and {@link ADXConstants#SERVICE_PRINCIPAL_AUTHORITY_ID_PROP}.
     * The Kusto Ingestion Client will only be created once.
     *
     * @return {@link Client} Kusto Ingestion Client for the configured ADX resource
     */
    public static synchronized Client getIngestionClient() {
        MetricsClient.timed(() -> {
            InvocationContext.getLogger().log(Level.FINE, "Fetching Kusto Ingestion Client.");
            if (ingestionClient == null) {
                ingestionClient = createClientWithUrl(System.getenv(ADXConstants.ADX_INGESTION_URI_PROP));
            }
        }, "ADXIngestionClientInit");

        return ingestionClient;
    }

    private static synchronized Client createClientWithUrl(String url) {
        Client client;
        ConnectionStringBuilder csb =
                ConnectionStringBuilder.createWithAadApplicationCredentials(
                        url,
                        System.getenv(ADXConstants.SERVICE_PRINCIPAL_APPLICATION_CLIENT_ID_PROP),
                        System.getenv(ADXConstants.SERVICE_PRINCIPAL_APPLICATION_KEY_PROP),
                        System.getenv(ADXConstants.SERVICE_PRINCIPAL_AUTHORITY_ID_PROP)
                );
        try {
            InvocationContext.getLogger().log(Level.FINE, String.format("Creating new Kusto Client with url: %s.", url));
            client = new ClientImpl(csb);
        } catch (URISyntaxException e) {
            // will be treated as permanent exception currently;
            // todo: Shall be enhanced to trigger circuit-breaker stopping the ingestion
            throw IoTRuntimeException.wrapNonTransient(IdentifierUtil.empty(), CommonErrorType.ADX_ERROR, String.format("ADX URI provided %s resulted in " +
                    "error %s", e.getInput(), e.getReason()), e);

        }

        return client;
    }
}