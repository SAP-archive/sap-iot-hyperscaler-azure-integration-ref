package com.sap.iot.azure.ref.integration.commons.mapping.token;

import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
import org.asynchttpclient.Request;

class RequestHelper {

    private RequestHelper() {
    }

    /**
     * Determine if the given {@link Request} is a replay request or not.
     *
     * @param request, request which is checked
     * @return whether or not the given request is a replay request
     */
    static boolean isReplayRequest(Request request) {
        return request.getHeaders().contains(MappingServiceConstants.X_REPLAY_HEADER, MappingServiceConstants.TRUE, true);
    }

}
