package com.sap.iot.azure.ref.integration.commons.mapping.token;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.MappingLookupException;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
import com.sap.iot.azure.ref.integration.commons.mapping.api.AsyncHttpClientFactory;
import com.sap.iot.azure.ref.integration.commons.mapping.util.HttpResponseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants.*;


public class TenantTokenCache {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final AsyncHttpClient asyncHttpClient;

    // token cached across function invocations
    private static String token;


    public TenantTokenCache() {
        this.asyncHttpClient = new AsyncHttpClientFactory().getAsyncHttpClient();
    }

    /**
     * Returns the current token.
     *
     * @return token as string
     */
    public synchronized String getToken() {
        if (token == null) {
            refreshToken();
        }

        return token;
    }

    /**
     * Refreshes and returns the token with the configured token endpoint and credentials.
     *
     * @return refreshed token as string
     */
    synchronized String refreshToken() {
        final String clientId = MappingServiceConstants.CLIENT_ID;
        final String clientSecret = MappingServiceConstants.CLIENT_SECRET;

        try {
            Response tokenResponse = getScopesRequestBuilder(clientId, clientSecret)
                    .addFormParam(REQUESTED_SCOPES, getRequiredScopes())
                    .execute()
                    .get();

            if (tokenResponse.getStatusCode() == HttpStatus.SC_BAD_REQUEST) { // invalid scopes are provided as env, fetch the token with all scopes
                InvocationContext.getLogger().warning(String.format("Invalid scopes %s provided in the function setting. Token fetch failed with response - " +
                        "%s. Will continue to fetch all scopes", SystemUtils.getEnvironmentVariable("sap-iot-required-api-scopes", ""),
                        tokenResponse.getResponseBody()));

                tokenResponse = getScopesRequestBuilder(clientId, clientSecret)
                        .execute()
                        .get();
            }

            HttpResponseUtil.validateHttpResponse(tokenResponse, "JWT token");

            token = objectMapper.readTree(tokenResponse.getResponseBody()).get(MappingServiceConstants.TOKEN_BODY_KEY).textValue();
        } catch (JsonParseException | JsonMappingException e) {
            throw new MappingLookupException("Error while fetching token", IdentifierUtil.getIdentifier(MappingServiceConstants.TOKEN_URL_PROPERTY_KEY,
                    MappingServiceConstants.TOKEN_ENDPOINT), false);
        } catch (IOException | ExecutionException e) {
            throw new MappingLookupException("Error while fetching token", IdentifierUtil.getIdentifier(MappingServiceConstants.TOKEN_URL_PROPERTY_KEY,
                    MappingServiceConstants.TOKEN_ENDPOINT), true);
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            throw new MappingLookupException("Error while fetching token due to an interruption",
                    IdentifierUtil.getIdentifier(MappingServiceConstants.TOKEN_URL_PROPERTY_KEY, MappingServiceConstants.TOKEN_ENDPOINT), true);
        }

        return token;
    }

    private String getRequiredScopes() {
        // return the scopes required for mapping & lookup apis
        String scopes = SystemUtils.getEnvironmentVariable("sap-iot-required-api-scopes", "");

        if (StringUtils.isEmpty(scopes)) {
            InvocationContext.getLogger().warning("Required scopes to access SAP IoT APIs is not maintained in the function settings. " +
                    "Please maintain minimum scopes required. Function will continue to fetch token with all scopes");

            return "";
        }

        return Stream.of(scopes.split(","))
                .map(String::trim)
                .collect(Collectors.joining(" "));
    }

    private BoundRequestBuilder getScopesRequestBuilder(String clientId, String clientSecret) {
        return asyncHttpClient.preparePost(MappingServiceConstants.TOKEN_ENDPOINT)
                .addFormParam(GRANT_TYPE, CLIENT_CREDENTIALS)
                .addFormParam(CLIENT_ID_KEY, clientId)
                .addFormParam(CLIENT_SECRET_KEY, clientSecret)
                .addFormParam(RESPONSE_TYPE, RESPONSE_TYPE_TOKEN)
                .addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
    }
}