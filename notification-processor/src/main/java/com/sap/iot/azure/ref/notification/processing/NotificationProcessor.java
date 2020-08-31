package com.sap.iot.azure.ref.notification.processing;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.retry.RetryTaskExecutor;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;
import com.sap.iot.azure.ref.notification.util.Constants;

import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

public interface NotificationProcessor {

    RetryTaskExecutor retryTaskExecutor = new RetryTaskExecutor();

    /**
     * Wraps the handleCreate method with a retry mechanism.
     *
     * @param notification, notification which is processed
     */
    default void handleCreateWithRetry(NotificationMessage notification) {
        try {
            retryTaskExecutor.executeWithRetry(() -> CompletableFuture.runAsync(InvocationContext.withContext(() -> handleCreate(notification))), Constants.MAX_RETRIES);
        } catch (Exception e) {
            InvocationContext.getContext().getLogger().log(Level.WARNING, String.format("%s - Processing of create notification failed after %s retries", this.getClass().getSimpleName(), Constants.MAX_RETRIES));
        }
    }

    /**
     * Wraps the handleUpdate method with a retry mechanism.
     *
     * @param notification, notification which is processed
     */
    default void handleUpdateWithRetry(NotificationMessage notification) {
        try {
            retryTaskExecutor.executeWithRetry(() -> CompletableFuture.runAsync(InvocationContext.withContext(() -> handleUpdate(notification))),
                    Constants.MAX_RETRIES);
        } catch (Exception e) {
            InvocationContext.getContext().getLogger().log(Level.SEVERE, String.format("%s - Processing of update notification failed after %s " +
                    "retries", this.getClass().getSimpleName(), Constants.MAX_RETRIES));
        }
    }

    /**
     * Wraps the handleDelete method with a retry mechanism.
     *
     * @param notification, notification which is processed
     */
    default void handleDeleteWithRetry(NotificationMessage notification) {
        try {
            retryTaskExecutor.executeWithRetry(() -> CompletableFuture.runAsync(InvocationContext.withContext(() -> handleDelete(notification))), Constants.MAX_RETRIES);
        } catch (Exception e) {
            InvocationContext.getContext().getLogger().log(Level.SEVERE, String.format("%s - Processing of delete notification failed after %s " +
                    "retries", this.getClass().getSimpleName(), Constants.MAX_RETRIES));
        }
    }

    void handleCreate(NotificationMessage notification) throws NotificationProcessException;

    void handleUpdate(NotificationMessage notification) throws NotificationProcessException;

    void handleDelete(NotificationMessage notification) throws NotificationProcessException;

}
