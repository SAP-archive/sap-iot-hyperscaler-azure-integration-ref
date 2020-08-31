package com.sap.iot.azure.ref.integration.commons.mapping.token;

import com.google.common.net.HttpHeaders;
import com.microsoft.azure.functions.HttpStatus;
import com.sap.iot.azure.ref.integration.commons.mapping.MappingServiceConstants;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.filter.FilterContext;
import org.asynchttpclient.filter.ResponseFilter;

public class MappingResponseFilter implements ResponseFilter {
    private final TenantTokenCache tenantTokenCache;

    public MappingResponseFilter() {
        this.tenantTokenCache = new TenantTokenCache();
    }

    /**
     * Check if the request failed due to an authorization issue.
     * If so, replay the request once with a refreshed token using the {@link TenantTokenCache}.
     * @param ctx, {@link FilterContext} containing the request
     * @return {@link FilterContext}
     */
    @Override
    public <T> FilterContext<T> filter(FilterContext<T> ctx) {
        // in case of unauthorized (401) errors, we try to fetch a new token and replay request once
        if (ctx.getResponseStatus().getStatusCode() == HttpStatus.UNAUTHORIZED.value()) {
            Request request = ctx.getRequest();
            // process only if this is a new 401 request. meaning, the replay header has not been set, or is set to false
            if (!RequestHelper.isReplayRequest(request)) {
                // fetch new token
                String token = this.tenantTokenCache.refreshToken();
                // add new headers
                Request replayRequest = new RequestBuilder(request)
                        .setHeader(HttpHeaders.AUTHORIZATION, MappingServiceConstants.BEARER_TOKEN_PREFIX + token)
                        .addHeader(MappingServiceConstants.X_REPLAY_HEADER, MappingServiceConstants.TRUE)
                        .build();

                return new FilterContext.FilterContextBuilder<T>()
                        .asyncHandler(ctx.getAsyncHandler())
                        .request(replayRequest)
                        .replayRequest(true)
                        .build();
            }
        }

        return ctx;
    }
}
