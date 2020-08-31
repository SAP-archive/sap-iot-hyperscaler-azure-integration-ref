package com.sap.iot.azure.ref.integration.commons.mapping.util;

import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.MappingLookupException;
import org.apache.http.HttpStatus;
import org.asynchttpclient.Response;
import org.asynchttpclient.netty.NettyResponseStatus;
import org.asynchttpclient.util.HttpConstants;

import java.time.Instant;

public class HttpResponseUtil {
    /**
     * Check header information and determines if the {@link Response} is valid.
     *
     * @param response,    response which is to be validated
     * @param serviceName, service name for forming possible error messages
     */
    public static void validateHttpResponse(Response response, String serviceName) throws MappingLookupException {
        int statusCode = response.getStatusCode();

        if (statusCode >= 500) { // 5xx series
            throw new MappingLookupException(String.format("API call for %s failed due to a server error with response: %s", serviceName, response.getResponseBody()),
                    IdentifierUtil.getIdentifier(MappingServiceConstants.SERVICE_NAME_KEY, serviceName), true);
        } else if (statusCode != HttpStatus.SC_OK) {
            throw new MappingLookupException(String.format("API call for %s failed with response: %s", serviceName, response.getResponseBody()),
                    IdentifierUtil.getIdentifier(MappingServiceConstants.SERVICE_NAME_KEY, serviceName), false);
        } else if (response.getResponseBody().isEmpty()) {
            throw new MappingLookupException(String.format("API call for %s returned with empty response body", serviceName),
                    IdentifierUtil.getIdentifier(MappingServiceConstants.SERVICE_NAME_KEY, serviceName), false);
        }
    }
}
