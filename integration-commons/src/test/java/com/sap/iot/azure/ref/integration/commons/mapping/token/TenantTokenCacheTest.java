package com.sap.iot.azure.ref.integration.commons.mapping.token;

import com.sap.iot.azure.ref.integration.commons.exception.MappingLookupException;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
import org.apache.http.HttpStatus;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.reset;

@RunWith(MockitoJUnitRunner.class)
public class TenantTokenCacheTest {

    private static final String TOKEN_URL = "sampleUrl";
    private final String SAMPLE_TOKEN = "sampleToken";
    private final String SAMPLE_PAYLOAD = String.format("{" +
            "    \"access_token\": \"%s\"," +
            "    \"token_type\": \"bearer\"," +
            "    \"expires_in\": 17999," +
            "    \"scope\": \"sampleScope\"," +
            "    \"jti\": \"4f557235316946f3b3ec0f8491a6bc70\"" +
            "}", SAMPLE_TOKEN);

    @Mock
    private AsyncHttpClient asyncHttpClient;
    @InjectMocks
    private TenantTokenCache tenantTokenCache;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @ClassRule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables()
            .set(MappingServiceConstants.TOKEN_ENDPOINT_PROP, TOKEN_URL);

    @After
    public void teardown() {
        reset(asyncHttpClient);
    }

    @Test
    public void testGetToken() throws ExecutionException, InterruptedException {
        mockTokenPayload();

        String token = tenantTokenCache.refreshToken();

        assertEquals(token, SAMPLE_TOKEN);
    }

    @Test
    public void testEmptyBody() throws ExecutionException, InterruptedException {
        mockTokenPayload("");

        expectedException.expect(MappingLookupException.class);
        expectedException.expectMessage("JWT token");
        expectedException.expectMessage("empty response body");

        tenantTokenCache.refreshToken();
    }

    @Test
    public void testInternalServerError() throws ExecutionException, InterruptedException {
        mockTokenPayload(SAMPLE_PAYLOAD, 500);

        expectedException.expect(MappingLookupException.class);
        expectedException.expectMessage("JWT token");
        expectedException.expectMessage("500");
        expectedException.expectMessage("server error");

        tenantTokenCache.refreshToken();
    }

    private void mockTokenPayload() throws ExecutionException, InterruptedException {
        mockTokenPayload(SAMPLE_PAYLOAD);
    }

    private void mockTokenPayload(String payload) throws ExecutionException, InterruptedException {
        mockTokenPayload(payload, HttpStatus.SC_OK);
    }

    private void mockTokenPayload(String payload, int statusCode) throws ExecutionException, InterruptedException {
        BoundRequestBuilder mockRequestBuilder = mock(BoundRequestBuilder.class);
        ListenableFuture mockFuture = mock(ListenableFuture.class);
        Response mockResponse = mock(Response.class);

        doReturn(mockRequestBuilder).when(asyncHttpClient).preparePost(eq(MappingServiceConstants.TOKEN_ENDPOINT));
        doReturn(mockRequestBuilder).when(mockRequestBuilder).addFormParam(anyString(), any());
        doReturn(mockRequestBuilder).when(mockRequestBuilder).addHeader(anyString(), anyString());

        doReturn(mockFuture).when(mockRequestBuilder).execute();
        doReturn(mockResponse).when(mockFuture).get();
        doReturn(payload).when(mockResponse).getResponseBody();

        doReturn(statusCode).when(mockResponse).getStatusCode();
    }
}