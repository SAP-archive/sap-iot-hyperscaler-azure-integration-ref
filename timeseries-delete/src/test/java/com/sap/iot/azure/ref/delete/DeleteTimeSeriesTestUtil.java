package com.sap.iot.azure.ref.delete;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class DeleteTimeSeriesTestUtil {
    public static final String REQUEST_ID = "SAMPLE_REQUEST_ID";
    public static final String CORRELATION_ID = "SAMPLE_CORRELATION_ID";
    public static final String SOURCE_ID = "SAMPLE_SOURCE_ID";
    public static final String STRUCTURE_ID = "SAMPLE_STRUCTURE_ID";
    public static final String FROM_TIMESTAMP = "2020-01-01T00:00:00.000Z";
    public static final String TO_TIMESTAMP = "2020-01-02T00:00:00.000Z";
    public static final String INGESTION_TIME = "2020-01-03T00:00:00.000Z";

    public static String createDeleteRequestWithPlaceHolders(String sampleMessage) throws IOException {
        return String.format(IOUtils.toString(DeleteTimeSeriesTestUtil.class.getResourceAsStream(sampleMessage), StandardCharsets.UTF_8), REQUEST_ID,
                CORRELATION_ID, SOURCE_ID, STRUCTURE_ID, INGESTION_TIME, FROM_TIMESTAMP, TO_TIMESTAMP);
    }

    public static String createSimpleDeleteRequest(String sampleMessage) throws IOException {
        return IOUtils.toString(DeleteTimeSeriesTestUtil.class.getResourceAsStream(sampleMessage), StandardCharsets.UTF_8);
    }
}
