package com.sap.iot.azure.ref.ingestion.mapping.util;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.MappingLookupException;
import com.sap.iot.azure.ref.integration.commons.mapping.util.HttpResponseUtil;
import org.apache.http.HttpStatus;
import org.asynchttpclient.Response;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class HttpResponseUtilTest {
    @Mock
    Response response;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testStatusCode() {
        int[] sampleStatusCodes = {HttpStatus.SC_BAD_REQUEST, HttpStatus.SC_FORBIDDEN, HttpStatus.SC_INTERNAL_SERVER_ERROR};
        for (int statusCode : sampleStatusCodes) {
            doReturn(statusCode).when(response).getStatusCode();
            expectedException.expect(MappingLookupException.class);
            HttpResponseUtil.validateHttpResponse(response, "sample");
        }
    }

    @Test
    public void testEmptyBody() {
        InvocationContextTestUtil.initInvocationContext();
        doReturn(HttpStatus.SC_OK).when(response).getStatusCode();
        doReturn("").when(response).getResponseBody();
        expectedException.expect(MappingLookupException.class);
        HttpResponseUtil.validateHttpResponse(response, "sample");
    }
}