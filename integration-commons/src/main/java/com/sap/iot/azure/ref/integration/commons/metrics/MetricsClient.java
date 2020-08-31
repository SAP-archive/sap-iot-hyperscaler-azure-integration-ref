package com.sap.iot.azure.ref.integration.commons.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.applicationinsights.TelemetryClient;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;

public class MetricsClient {

    public static final boolean PERF_METRICS_ENABLED = Boolean.parseBoolean(System.getenv("enable-perf-analysis-metrics"));

    @VisibleForTesting
    private static TelemetryClient telemetryClient = new TelemetryClient();

    /**
     * publish metric to AppInsights. Metrics will be published only if "enable-perf-metrics" is set to true
     * @param metricName name of the metric - naming conversion for metric name is "{FunctionName} C_{MetricName}"
     * @param value value
     */
    public static void trackPerfMetric(String metricName, long value) {
        if (PERF_METRICS_ENABLED) {
            trackMetric(metricName, value);
        }
    }

    /**
     * publish metric to AppInsights. Metrics will be published only if "enable-perf-metrics" is set to true
     * @param metricName name of the metric - naming conversion for metric name is "{FunctionName} C_{MetricName}"
     * @param value value
     */
    public static void trackMetric(String metricName, long value) {
        // enhance with checking env if metric enabled
        telemetryClient.trackMetric(metricName, value);
    }

    /**
     * returns the metric name following the convention - "{FunctionName} C_{MetricName}"
     * @param metricName metric name
     * @return formatted metric name
     */
    public static String getMetricName(String metricName) {
        return String.format("%s C_%s", InvocationContext.getContext().getFunctionName(), metricName);
    }

    /**
     * executes the given runnable with capturing the
     * @param runnable task to run
     * @param metricName metric name
     */
    public static void timed(Runnable runnable, String metricName) {

        long start = System.currentTimeMillis();
        try {
            runnable.run();
        } finally {
            telemetryClient.trackMetric(getMetricName(metricName), (double)System.currentTimeMillis() - start);
        }
    }
}
