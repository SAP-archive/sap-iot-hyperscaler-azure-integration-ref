package com.sap.iot.azure.ref.integration.commons.context;

import java.util.Objects;
import java.util.logging.Filter;
import java.util.logging.LogRecord;

public class LoggingMessageFilter implements Filter {

    private final Filter origFilter;

    private LoggingMessageFilter(Filter origFilter) {
        this.origFilter = origFilter;
    }

    /**
     * returns logging originalFilter with enriched message only if the original filter is not of the same type
     * @param originalFilter original filter
     * @return new / same originalFilter
     */
    static Filter getEnrichedFilter(Filter originalFilter) {
        if (originalFilter instanceof LoggingMessageFilter) return originalFilter;
        else return new LoggingMessageFilter(originalFilter);
    }

    @Override
    public boolean isLoggable(LogRecord logRecord) {

        if (logRecord == null) {
            return Objects.isNull(origFilter) || origFilter.isLoggable(null);
        }

        logRecord.setMessage(String.format("[%s %s] %s", logRecord.getSourceClassName(), logRecord.getSourceMethodName(), logRecord.getMessage()));
        return Objects.isNull(origFilter) || origFilter.isLoggable(logRecord);
    }
}
