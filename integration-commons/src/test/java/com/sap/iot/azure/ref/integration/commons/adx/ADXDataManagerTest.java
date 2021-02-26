package com.sap.iot.azure.ref.integration.commons.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.Results;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ADXDataManagerTest {

    @Mock
    private Client kustoClient;
    @Mock
    private Client kustoIngestionClient;

    private final String STRUCTURE_ID = "STRUC_ID";
    private final String OPERATION_ID = "OP_ID";
    private final String TABLE_NAME = ADXConstants.TABLE_PREFIX + STRUCTURE_ID;
    private final String COLUMNN_NAME = "sampleColumnName";
    private final String DATA_EXISTS_QUERY = String.format("%s | take 1", TABLE_NAME);
    private final String COLUMN_DATA_EXISTS_QUERY = String.format("%s | project %s | where isnotempty(%s) | take 1", TABLE_NAME, COLUMNN_NAME, COLUMNN_NAME);

    private final static String DB_NAME = "sampledb";
    //Purge Query
    private final String SOURCE_ID = "sourceId";
    private final String OTHER_SOURCE_ID = "sourceId2";
    private final String FROM_TIMESTAMP = "2020-01-01T00:00:00Z";
    private final String TO_TIMESTAMP = "2020-01-02T00:00:00Z";
    private final String DELETE_REQUEST_TIMESTAMP = "2020-01-03T00:00:00Z";

    private ADXDataManager adxDataManager;

    @ClassRule
    public static final EnvironmentVariables environmentVariables = new EnvironmentVariables()
            .set(ADXConstants.ADX_DATABASE_NAME_PROP, DB_NAME);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupClass() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Before
    public void setupTest() {
        adxDataManager = new ADXDataManager(kustoClient, kustoIngestionClient);
    }

    @Test
    public void testDataExists() throws DataClientException, DataServiceException {
        doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), eq(DATA_EXISTS_QUERY));
        boolean dataExists1 = adxDataManager.dataExists(STRUCTURE_ID);
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), eq(DATA_EXISTS_QUERY));
        boolean dataExists2 = adxDataManager.dataExists(STRUCTURE_ID);

        assertEquals(true, dataExists1);
        assertEquals(false, dataExists2);
    }

    @Test
    public void testColumnDataExists() throws DataClientException, DataServiceException {
        doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), eq(COLUMN_DATA_EXISTS_QUERY));
        boolean dataExists1 = adxDataManager.dataExistsForColumn(STRUCTURE_ID, COLUMNN_NAME);
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), eq(COLUMN_DATA_EXISTS_QUERY));
        boolean dataExists2 = adxDataManager.dataExistsForColumn(STRUCTURE_ID, COLUMNN_NAME);

        assertEquals(true, dataExists1);
        assertEquals(false, dataExists2);
    }

    @Test
    public void testDeleteTimeSeries() throws DataClientException, DataServiceException {
        String deleteTimeSeriesQueryWithoutSourceId = String.format(".set-or-append async %s <| %s | where (_time >= datetime(\"%s\") and _time " +
                        "<= datetime(\"%s\")) and _enqueued_time < datetime(\"%s\") | where _isDeleted != true | extend " +
                        "_enqueued_time = now(), _isDeleted = true", TABLE_NAME,
                TABLE_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, DELETE_REQUEST_TIMESTAMP);
        String deleteTimeSeriesQueryInclusive = String.format(".set-or-append async %s <| %s | where (_time >= datetime(\"%s\") and _time " +
                        "<= datetime(\"%s\")) and sourceId in (\"%s\") and _enqueued_time < datetime(\"%s\") | where _isDeleted != true | extend " +
                        "_enqueued_time = now(), _isDeleted = true", TABLE_NAME,
                TABLE_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, SOURCE_ID, DELETE_REQUEST_TIMESTAMP);
        String deleteTimeSeriesQueryMultipleSourceIds = String.format(".set-or-append async %s <| %s | where (_time >= datetime(\"%s\") and _time " +
                        "<= datetime(\"%s\")) and sourceId in (\"%s\", \"%s\") and _enqueued_time < datetime(\"%s\") | where _isDeleted != true | extend " +
                        "_enqueued_time = now(), _isDeleted = true", TABLE_NAME,
                TABLE_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, SOURCE_ID, OTHER_SOURCE_ID, DELETE_REQUEST_TIMESTAMP);
        String deleteTimeSeriesQueryNonInclusive = String.format(".set-or-append async %s <| %s | where (_time > datetime(\"%s\") and _time " +
                        "< datetime(\"%s\")) and sourceId in (\"%s\") and _enqueued_time < datetime(\"%s\") | where _isDeleted != true | extend " +
                        "_enqueued_time = now(), _isDeleted = true", TABLE_NAME,
                TABLE_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, SOURCE_ID, DELETE_REQUEST_TIMESTAMP);

        //mock results
        doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), anyString());

        //no source ID test
        adxDataManager.deleteTimeSeries(getDeleteRequest(true, true, Collections.EMPTY_LIST));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(deleteTimeSeriesQueryWithoutSourceId));

        //multiple source IDs
        adxDataManager.deleteTimeSeries(getDeleteRequest(true, true, Arrays.asList(SOURCE_ID, OTHER_SOURCE_ID)));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(deleteTimeSeriesQueryMultipleSourceIds));

        //inclusive test
        adxDataManager.deleteTimeSeries(getDeleteRequest(true, true, Collections.singletonList(SOURCE_ID)));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(deleteTimeSeriesQueryInclusive));

        //non inclusive test
        adxDataManager.deleteTimeSeries(getDeleteRequest(false, false, Collections.singletonList(SOURCE_ID)));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(deleteTimeSeriesQueryNonInclusive));
    }

    @Test
    public void testDeleteTimeSeriesWithNoSourceId() throws DataClientException, DataServiceException {
        String deleteTimeSeriesQueryWithoutSourceId = String.format(".set-or-append async %s <| %s | where (_time >= datetime(\"%s\") and _time " +
                        "<= datetime(\"%s\")) and _enqueued_time < datetime(\"%s\") | where _isDeleted != true | extend " +
                        "_enqueued_time = now(), _isDeleted = true", TABLE_NAME,
                TABLE_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, DELETE_REQUEST_TIMESTAMP);

        //mock results
        doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), anyString());

        //no source ID test
        adxDataManager.deleteTimeSeries(getDeleteRequest(true, true, Collections.EMPTY_LIST));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(deleteTimeSeriesQueryWithoutSourceId));
    }

    @Test
    public void testDeleteTimeSeriesWithMultipleSourceIds() throws DataClientException, DataServiceException {
        String deleteTimeSeriesQueryMultipleSourceIds = String.format(".set-or-append async %s <| %s | where (_time >= datetime(\"%s\") and _time " +
                        "<= datetime(\"%s\")) and sourceId in (\"%s\", \"%s\") and _enqueued_time < datetime(\"%s\") | where _isDeleted != true | extend " +
                        "_enqueued_time = now(), _isDeleted = true", TABLE_NAME,
                TABLE_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, SOURCE_ID, OTHER_SOURCE_ID, DELETE_REQUEST_TIMESTAMP);

        //mock results
        doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), anyString());

        //multiple source IDs
        adxDataManager.deleteTimeSeries(getDeleteRequest(true, true, Arrays.asList(SOURCE_ID, OTHER_SOURCE_ID)));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(deleteTimeSeriesQueryMultipleSourceIds));
    }

    @Test
    public void testDeleteTimeSeriesNoOperationIdReturned() throws DataClientException, DataServiceException {
        //mock empty results
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), anyString());

        //ADX client exception should be thrown
        expectedException.expect(ADXClientException.class);
        expectedException.expectMessage(CommonConstants.STRUCTURE_ID_PROPERTY_KEY);
        expectedException.expectMessage(STRUCTURE_ID);
        adxDataManager.deleteTimeSeries(getDeleteRequest(true, true, Collections.EMPTY_LIST));
    }

    @Test
    public void testDeleteTimeSeriesWithInclusiveTimestamps() throws DataClientException, DataServiceException {
        String deleteTimeSeriesQueryInclusive = String.format(".set-or-append async %s <| %s | where (_time >= datetime(\"%s\") and _time " +
                        "<= datetime(\"%s\")) and sourceId in (\"%s\") and _enqueued_time < datetime(\"%s\") | where _isDeleted != true | extend " +
                        "_enqueued_time = now(), _isDeleted = true", TABLE_NAME,
                TABLE_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, SOURCE_ID, DELETE_REQUEST_TIMESTAMP);

        //mock results
        doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), anyString());

        //inclusive test
        adxDataManager.deleteTimeSeries(getDeleteRequest(true, true, Collections.singletonList(SOURCE_ID)));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(deleteTimeSeriesQueryInclusive));
    }

    @Test
    public void testDeleteTimeSeriesWithNonInclusiveTimestamps() throws DataClientException, DataServiceException {
        String deleteTimeSeriesQueryNonInclusive = String.format(".set-or-append async %s <| %s | where (_time > datetime(\"%s\") and _time " +
                        "< datetime(\"%s\")) and sourceId in (\"%s\") and _enqueued_time < datetime(\"%s\") | where _isDeleted != true | extend " +
                        "_enqueued_time = now(), _isDeleted = true", TABLE_NAME,
                TABLE_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, SOURCE_ID, DELETE_REQUEST_TIMESTAMP);

        //mock results
        doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), anyString());

        //non inclusive test
        adxDataManager.deleteTimeSeries(getDeleteRequest(false, false, Collections.singletonList(SOURCE_ID)));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(deleteTimeSeriesQueryNonInclusive));
    }

    @Test
    public void testPurgeWithoutSourceId() throws DataClientException, DataServiceException {
        String purgeQueryWithoutSourceId = String.format(".purge table [%s] records in database [%s] with (noregrets='true') <| " +
                "where (_time >= (datetime(\"%s\")) " +
                "and _time <= (datetime(\"%s\")) " +
                "and _enqueued_time < (datetime(\"%s\")))", TABLE_NAME, DB_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, DELETE_REQUEST_TIMESTAMP);

        //mock results
        doReturn(getSampleResultForOperationCheck(true, "completed")).when(kustoIngestionClient).execute(anyString());

        //no source ID test
        adxDataManager.purgeTimeSeries(STRUCTURE_ID, Collections.singletonList(getDeleteRequest(true, true, Collections.EMPTY_LIST)));
        verify(kustoIngestionClient, times(1)).execute(eq(purgeQueryWithoutSourceId));
    }

    @Test
    public void testPurgeWithMultipleSourceIds() throws DataClientException, DataServiceException {
        String purgeQueryMultipleSourceIds = String.format(".purge table [%s] records in database [%s] with (noregrets='true') <| " +
                        "where (_time >= (datetime(\"%s\")) " +
                        "and _time <= (datetime(\"%s\")) " +
                        "and sourceId in (\"%s\", \"%s\") " +
                        "and _enqueued_time < (datetime(\"%s\")))", TABLE_NAME, DB_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, SOURCE_ID, OTHER_SOURCE_ID,
                DELETE_REQUEST_TIMESTAMP);

        //mock results
        doReturn(getSampleResultForOperationCheck(true, "completed")).when(kustoIngestionClient).execute(anyString());

        //multiple source IDs
        adxDataManager.purgeTimeSeries(STRUCTURE_ID, Collections.singletonList(getDeleteRequest(true, true, Arrays.asList(SOURCE_ID, OTHER_SOURCE_ID))));
        verify(kustoIngestionClient, times(1)).execute(eq(purgeQueryMultipleSourceIds));
    }

    @Test
    public void testPurgeWithInclusiveTimestamps() throws DataClientException, DataServiceException {
        String purgeQueryInclusive = String.format(".purge table [%s] records in database [%s] with (noregrets='true') <| " +
                "where (_time >= (datetime(\"%s\")) " +
                "and _time <= (datetime(\"%s\")) " +
                "and sourceId in (\"%s\") " +
                "and _enqueued_time < (datetime(\"%s\")))", TABLE_NAME, DB_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, SOURCE_ID, DELETE_REQUEST_TIMESTAMP);

        //mock results
        doReturn(getSampleResultForOperationCheck(true, "completed")).when(kustoIngestionClient).execute(anyString());

        //inclusive test
        adxDataManager.purgeTimeSeries(STRUCTURE_ID, Collections.singletonList(getDeleteRequest(true, true, Collections.singletonList(SOURCE_ID))));
        verify(kustoIngestionClient, times(1)).execute(eq(purgeQueryInclusive));
    }

    @Test
    public void testPurgeWithNonInclusiveTimestamps() throws DataClientException, DataServiceException {
        String purgeQueryNonInclusive = String.format(".purge table [%s] records in database [%s] with (noregrets='true') <| " +
                "where (_time > (datetime(\"%s\")) " +
                "and _time < (datetime(\"%s\")) " +
                "and sourceId in (\"%s\") " +
                "and _enqueued_time < (datetime(\"%s\")))", TABLE_NAME, DB_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, SOURCE_ID, DELETE_REQUEST_TIMESTAMP);

        //mock results
        doReturn(getSampleResultForOperationCheck(true, "completed")).when(kustoIngestionClient).execute(anyString());

        //non inclusive test
        adxDataManager.purgeTimeSeries(STRUCTURE_ID, Collections.singletonList(getDeleteRequest(false, false, Collections.singletonList(SOURCE_ID))));
        verify(kustoIngestionClient, times(1)).execute(eq(purgeQueryNonInclusive));
    }

    @Test
    public void testPurgeNoOperationIdReturned() throws DataClientException, DataServiceException {
        //mock empty results
        doReturn(getSampleResults(false)).when(kustoIngestionClient).execute(anyString());

        //ADX client exception should be thrown
        expectedException.expect(ADXClientException.class);
        expectedException.expectMessage(CommonConstants.STRUCTURE_ID_PROPERTY_KEY);
        expectedException.expectMessage(STRUCTURE_ID);
        adxDataManager.purgeTimeSeries(STRUCTURE_ID, Collections.singletonList(getDeleteRequest(true, true, Collections.EMPTY_LIST)));
    }

    @Test
    public void testPurgeException() throws DataClientException, DataServiceException {
        doThrow(DataClientException.class).when(kustoIngestionClient).execute(anyString());

        //Exceptions when creating adx resources are wrapped as IngestionRuntimeException
        expectedException.expect(ADXClientException.class);
        expectedException.expectMessage(CommonConstants.STRUCTURE_ID_PROPERTY_KEY);
        expectedException.expectMessage(STRUCTURE_ID);
        adxDataManager.purgeTimeSeries(STRUCTURE_ID, Collections.singletonList(getDeleteRequest(true, true, Collections.EMPTY_LIST)));
    }

    @Test
    public void testMultiplePurgeWithoutSourceId() throws DataClientException, DataServiceException {
        String purgeQueryWithoutSourceId = String.format(".purge table [%s] records in database [%s] with (noregrets='true') <| " +
                "where (_time >= (datetime(\"%s\")) " +
                "and _time <= (datetime(\"%s\")) " +
                "and _enqueued_time < (datetime(\"%s\"))) " +
                "or (_time >= (datetime(\"%s\")) " +
                "and _time <= (datetime(\"%s\")) " +
                "and _enqueued_time < (datetime(\"%s\")))", TABLE_NAME, DB_NAME, FROM_TIMESTAMP, TO_TIMESTAMP, DELETE_REQUEST_TIMESTAMP, FROM_TIMESTAMP, TO_TIMESTAMP, DELETE_REQUEST_TIMESTAMP);
        List<DeleteInfo> deleteInfos = Arrays.asList(getDeleteRequest(true, true, Collections.EMPTY_LIST), getDeleteRequest(true, true,
                Collections.EMPTY_LIST));

        //mock results
        doReturn(getSampleResultForOperationCheck(true, "completed")).when(kustoIngestionClient).execute(anyString());

        //no source ID test
        adxDataManager.purgeTimeSeries(STRUCTURE_ID, deleteInfos);
        verify(kustoIngestionClient, times(1)).execute(eq(purgeQueryWithoutSourceId));
    }

    @Test
    public void testOperationStatusException() throws DataClientException, DataServiceException {
        doThrow(DataClientException.class).when(kustoClient).execute(anyString());
        //ADX client exception should be thrown
        expectedException.expect(ADXClientException.class);
        expectedException.expectMessage("Exception in accessing ADX artifacts");
        expectedException.expectMessage(CommonConstants.STRUCTURE_ID_PROPERTY_KEY);
        expectedException.expectMessage(STRUCTURE_ID);
        adxDataManager.getDeleteOperationStatus(OPERATION_ID, STRUCTURE_ID);
    }

    @Test
    public void testDeleteOperationStatus() throws DataClientException, DataServiceException {
        doReturn(getSampleResultForOperationCheck(true, "Completed")).when(kustoClient).execute(anyString());

        DeleteOperationStatus status = adxDataManager.getDeleteOperationStatus("", "");

        assertEquals(DeleteOperationStatus.COMPLETED, status);
    }

    @Test
    public void testEmptyDeleteOperationStatus() throws DataClientException, DataServiceException {
        doReturn(getSampleResultForOperationCheck(false, "")).when(kustoClient).execute(anyString());

        expectedException.expect(ADXClientException.class);
        expectedException.expectMessage(CommonConstants.OPERATION_ID);
        expectedException.expectMessage(CommonConstants.STRUCTURE_ID_PROPERTY_KEY);
        expectedException.expectMessage(OPERATION_ID);
        expectedException.expectMessage(STRUCTURE_ID);

        DeleteOperationStatus status = adxDataManager.getDeleteOperationStatus(OPERATION_ID, STRUCTURE_ID);
    }

    @Test
    public void testPurgeOperationStatus() throws DataClientException, DataServiceException {
        doReturn(getSampleResultForOperationCheck(true, "Completed")).when(kustoIngestionClient).execute(anyString());

        DeleteOperationStatus status = adxDataManager.getPurgeOperationStatus("", "");

        assertEquals(DeleteOperationStatus.COMPLETED, status);
    }

    @Test
    public void testEmptyPurgeOperationStatus() throws DataClientException, DataServiceException {
        doReturn(getSampleResultForOperationCheck(false, "")).when(kustoIngestionClient).execute(anyString());

        expectedException.expect(ADXClientException.class);
        expectedException.expectMessage(CommonConstants.OPERATION_ID);
        expectedException.expectMessage(CommonConstants.STRUCTURE_ID_PROPERTY_KEY);
        expectedException.expectMessage(STRUCTURE_ID);
        expectedException.expectMessage(OPERATION_ID);
        DeleteOperationStatus status = adxDataManager.getPurgeOperationStatus(OPERATION_ID, STRUCTURE_ID);
    }

    private Results getSampleResults(boolean filled) {
        ArrayList<String> innerValues = new ArrayList<>();
        ArrayList<ArrayList<String>> values = new ArrayList<>();
        Results results = new Results(null, null, values, null);

        innerValues.add("TEST");
        if (filled) {
            values.add(innerValues);
        }

        return results;
    }

    private DeleteInfo getDeleteRequest(boolean fromTimestampInclusive, boolean toTimestampInclusive, List sourceIds) {
        return DeleteInfo.builder()
                .structureId(STRUCTURE_ID)
                .sourceIds(sourceIds)
                .fromTimestamp(FROM_TIMESTAMP)
                .toTimestamp(TO_TIMESTAMP)
                .ingestionTimestamp(DELETE_REQUEST_TIMESTAMP)
                .fromTimestampInclusive(fromTimestampInclusive)
                .toTimestampInclusive(toTimestampInclusive)
                .build();
    }

    private Results getSampleResultForOperationCheck(boolean filled, String state) {
        ArrayList<ArrayList<String>> values = new ArrayList<>();
        ArrayList<String> innerValues = new ArrayList<>();
        HashMap<String, Integer> columnNameToIndex = new HashMap<>();
        Results results = new Results(columnNameToIndex, null, values, null);

        columnNameToIndex.put("State", 0);
        innerValues.add(state);
        if (filled) {
            values.add(innerValues);
        }

        return results;
    }
}