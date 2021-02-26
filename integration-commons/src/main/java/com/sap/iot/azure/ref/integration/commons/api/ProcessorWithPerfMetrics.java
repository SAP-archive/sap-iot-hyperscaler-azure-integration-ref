package com.sap.iot.azure.ref.integration.commons.api;

import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import com.sap.iot.azure.ref.integration.commons.metrics.MetricsClient;

/**
 * This interface extends the functions of {@link Processor} interface with capturing performance metrics
 *
 * If a permanent exception is caught, the exception will be logged and null will be returned.
 * If a transient exception is caught, the exception will be rethrown. This allows for further exception handling like a retry mechanism
 *
 * @param <T>, generic input parameter
 * @param <R>, generic output parameter
 */
public interface ProcessorWithPerfMetrics<T, R> extends Processor<T, R> {

    @Override
    default R apply(T t) throws IoTRuntimeException {
        long then = System.currentTimeMillis();
        R res = Processor.super.apply(t);
        MetricsClient.trackPerfMetric(MetricsClient.getMetricName(this.getClass().getSimpleName()), System.currentTimeMillis() - then);
        return res;
    }
}
