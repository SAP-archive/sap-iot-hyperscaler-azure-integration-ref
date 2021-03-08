package com.sap.iot.azure.ref.integration.commons.api;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.metrics.MetricsClient;

import java.util.function.Function;
import java.util.logging.Level;

/**
 * This interface adds basic handling of {@link IoTRuntimeException} and entry-exit logs for each process implemented.
 * If a permanent exception is caught, the exception will be logged and null will be returned.
 * If a transient exception is caught, the exception will be rethrown. This allows for further exception handling like a retry mechanism
 *
 * @param <T>, generic input parameter
 * @param <R>, generic output parameter
 */
public interface Processor<T, R> extends Function<T, R> {

    R process(T t) throws IoTRuntimeException;

    @Override
    default R apply(T t) throws IoTRuntimeException {
        try {
            // wrap the actual process invocation with entering & exit logs
            // Not using log.entering / log.exiting method as the placeholder are not replaced with parameters for the ExecutionContext Logger
            InvocationContext.getLogger().finer(() -> String.format("ENTERING Class: %s; Method: %s; Parameter: %s  ", this.getClass().getName(), "process",
                    t.toString()));

            R res = process(t);

            // expects that the parameter has a valid toString method implementation
            InvocationContext.getLogger().finer(() -> String.format("EXITING Class: %s; Method: %s; Parameter: %s", this.getClass().getName(), "process",
                    t.toString()));

            return res;
        } catch (IoTRuntimeException ex) {
            // in case of mapping exception - log the error and proceed to next message processing
            if (!ex.isTransient()) {
                InvocationContext.getLogger().log(Level.SEVERE, ex.jsonify().toString(), ex);
                return null;
            } else {
                // exception with type=transient and all other runtime exception is treated as transient and is propagated to be handled by RetryExecutor
                throw ex;
            }
        }
    }
}
