package com.sap.iot.azure.ref.integration.commons.mapping.token;

import com.microsoft.azure.functions.HttpStatus;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.filter.FilterContext;
import org.asynchttpclient.filter.FilterException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MappingResponseFilterTest {
    @Mock
    FilterContext mockContext;
    @Mock
    HttpResponseStatus mockResponseStatus;

    @BeforeClass
    public static void setupClass() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Test
    public void testAuthorized() throws FilterException {
        doReturn(HttpStatus.ACCEPTED.value()).when(mockResponseStatus).getStatusCode();
        doReturn(mockResponseStatus).when(mockContext).getResponseStatus();

        MappingResponseFilter mappingResponseFilter = new MappingResponseFilter();
        mappingResponseFilter.filter(mockContext);
        verify(mockResponseStatus, times(1)).getStatusCode();
    }
}