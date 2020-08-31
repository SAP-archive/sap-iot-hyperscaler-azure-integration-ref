package com.sap.iot.azure.ref.integration.commons.retry;

import com.google.common.annotations.VisibleForTesting;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.concurrent.*;
import java.util.logging.Level;

public class RetryTaskExecutor {
    private static ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(8);

    /**
     * Execute a provided {@link Callable} with a configurable number of retries.
     * The provided {@link Callable} returns a {@link CompletableFuture}.
     * If the {@link CompletableFuture} completes exceptionally, a retry will be scheduled with a delay and exponentially back off.
     * The delay starts with one second and doubles for every further retry.
     * If the execution fails for more times than the given retries, the returned {@link CompletableFuture} completes exceptionally
     *
     * @param callable,   will be executed. Has to return {@link CompletableFuture}.
     * @param maxRetries, number of retries in case of exceptional completion of {@link CompletableFuture}
     * @return {@link CompletableFuture}
     */
    public <T> CompletableFuture<T> executeWithRetry(Callable<CompletableFuture<T>> callable, int maxRetries) {
        CompletableFuture<T> future = new CompletableFuture<>();

        try {
            future = callable.call();

            for (int i = 1; i < maxRetries; i++) {
                final int currDelay = getNextDelay(i);
                final int retryCount = i;

                future = future.exceptionally(cause -> {
                    CompletableFuture<T> resCF = new CompletableFuture<>();

                    int iotRunTimeExIndex = ExceptionUtils.indexOfType(cause, IoTRuntimeException.class);
                    if (iotRunTimeExIndex >= 0) {
                        // if the cause or the most recent exception in the stack is marked as permanent, no more retries will be executed
                        IoTRuntimeException iotRuntimeException = (IoTRuntimeException) ExceptionUtils.getThrowableList(cause).get(iotRunTimeExIndex);
                        if (!iotRuntimeException.isTransient()) {
                            InvocationContext.getLogger().log(Level.WARNING, String.format("Permanent Error %s occurred in retry task; shall not retried further",
                                    iotRuntimeException.getErrorType()));
                            throw iotRuntimeException; // not scheduled any further for next retries
                        }
                    }

                    scheduledExecutor.schedule(InvocationContext.withContext((Callable<Boolean>) () -> {
                        try {
                            InvocationContext.getLogger().log(Level.WARNING, String.format("Running new attempt: %s", retryCount));
                            callable.call().get();

                            return resCF.complete(null);
                        } catch (Exception ex) {
                            InvocationContext.getLogger().log(Level.WARNING, String.format("Transient error - will retry after %s seconds", currDelay), ex);
                            return resCF.completeExceptionally(ex);
                        }
                    }), currDelay, TimeUnit.SECONDS);

                    return resCF.join();
                });
            }
        } catch (Exception ex) {
            future.completeExceptionally(ex);
        }

        return future;
    }

    @VisibleForTesting
    int getNextDelay(int counter) {
        return (int) Math.pow(2, counter);
    }
}