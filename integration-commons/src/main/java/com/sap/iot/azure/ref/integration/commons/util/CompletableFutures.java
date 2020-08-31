package com.sap.iot.azure.ref.integration.commons.util;

import java.util.concurrent.CompletableFuture;

public class CompletableFutures {

    /**
     * restrict initialization
     */
    private CompletableFutures() {

    }

    /**
     * returns a completable future that's already completed with null value
     * suppresses the findbug violation for using null parameters when invoking {@code CompletableFuture.completedFuture(null)}
     * @return completed future with null value
     */
    public static CompletableFuture<Void> voidCompletedFuture() {
       return CompletableFuture.allOf();
    }

    /**
     * returns a failed future with the given {@param ex}
     * @param ex returned future fails with {@param ex}
     * @param <T> completable future type
     * @return completable future
     */
    public static <T> CompletableFuture<T> completeExceptionally(Exception ex) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        cf.completeExceptionally(ex);
        return cf;
    }
}
