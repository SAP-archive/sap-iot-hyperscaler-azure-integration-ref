package com.sap.iot.azure.ref.integration.commons.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.Results;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.sap.iot.azure.ref.integration.commons.avro.AvroConstants;
import com.sap.iot.azure.ref.integration.commons.avro.TestAVROSchemaConstants;
import com.sap.iot.azure.ref.integration.commons.constants.IngestionType;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.exception.AvroIngestionException;
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

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ADXTableManagerTest {

    @Mock
    private Client kustoClient;
    @Mock
    Clock clock;

    private final String STRUCTURE_ID = "STRUC_ID";
    private final String TABLE_NAME = ADXConstants.TABLE_PREFIX + STRUCTURE_ID;
    private final String COLUMNN_NAME = "sampleColumnName";
    private final long MOCK_MILLIS = 12345;
    private final String NEW_COLUMNN_NAME = COLUMNN_NAME + String.format(KQLQueries.SOFT_DELETE_SUFFIX, MOCK_MILLIS);
    private final String NEW_TABLE_NAME = TABLE_NAME + String.format(KQLQueries.SOFT_DELETE_SUFFIX, MOCK_MILLIS);
    private final String COLUMNN_DATA_TYPE = "sampleDataType";
    private final static String DB_NAME = "sampledb";
    private final String SIMPLE_TABLE_CREATION_QUERY = String.format(".create-merge table %s (sourceId: string, _enqueued_time: datetime, _isDeleted: bool)",
            TABLE_NAME);
    private final String BATCHING_POLICY_CREATION_QUERY = String.format(KQLQueries.CREATE_BATCHING_POLICY_QUERY, TABLE_NAME, KQLQueries.DEFAULT_MAX_NUMBER_OF_ITEMS,
            KQLQueries.DEFAULT_MAX_RAW_DATA_SIZE);
    private final String STREAMING_POLICY_CREATION_QUERY = String.format(KQLQueries.CREATE_STREAMING_POLICY_QUERY, TABLE_NAME);
    private final String COMPLEX_TABLE_CREATION_QUERY = String.format(".create-merge table %s (sourceId: string, _enqueued_time: datetime, _isDeleted: bool, " +
            "_time:" +
            " datetime, decimalMeasure: decimal, booleanMeasure: bool, intMeasure: int, longMeasure: long, floatMeasure: real, tag1: string, tag2: string, tag3: string, tag4: string)", TABLE_NAME);
    private final String SIMPLE_MAPPING_CREATION_QUERY = String.format(".create-or-alter table %s ingestion json mapping '%s' '[{\"path\":\"$.sourceId\"," +
                    "\"column\":\"sourceId\"},{\"path\":\"$.x-opt-enqueued-time\",\"column\":\"_enqueued_time\"},{\"path\":\"false\",\"column\":\"_isDeleted\"}]'",
            TABLE_NAME, TABLE_NAME);
    private final String COMPLEX_MAPPING_CREATION_QUERY = String.format(".create-or-alter table %s ingestion json mapping '%s' '[{\"path\":\"$.sourceId\"," +
            "\"column\":\"sourceId\"},{\"path\":\"$.x-opt-enqueued-time\",\"column\":\"_enqueued_time\"},{\"path\":\"false\",\"column\":\"_isDeleted\"},{\"path\":\"$._time\",\"column\":\"_time\"},{\"path\":\"$.measurements.decimalMeasure\",\"column\":\"decimalMeasure\"},{\"path\":\"$.measurements.booleanMeasure\",\"column\":\"booleanMeasure\"},{\"path\":\"$.measurements.intMeasure\",\"column\":\"intMeasure\"},{\"path\":\"$.measurements.longMeasure\",\"column\":\"longMeasure\"},{\"path\":\"$.measurements.floatMeasure\",\"column\":\"floatMeasure\"},{\"path\":\"$.measurements.tag1\",\"column\":\"tag1\"},{\"path\":\"$.measurements.tag2\",\"column\":\"tag2\"},{\"path\":\"$.measurements.tag3\",\"column\":\"tag3\"},{\"path\":\"$.measurements.tag4\",\"column\":\"tag4\"}]'", TABLE_NAME, TABLE_NAME);
    private final String UPDATE_COLUMN_QUERY = String.format(".alter column ['%s'].['%s'] type=%s", TABLE_NAME, COLUMNN_NAME, COLUMNN_DATA_TYPE);
    private final String DROP_TABLE_QUERY = String.format(".drop table %s ifexists", TABLE_NAME);
    private final String DROP_COLUMN_QUERY = String.format(".drop column %s . %s", TABLE_NAME, COLUMNN_NAME);
    private final String SIMPLE_TABLE_UPDATE_QUERY = String.format(".alter-merge table %s (sourceId: string, _enqueued_time: datetime, _isDeleted: bool)",
            TABLE_NAME);
    private final String RENAME_COLUMN_QUERY = String.format(".rename column %s . %s to %s", TABLE_NAME, COLUMNN_NAME, NEW_COLUMNN_NAME);
    private final String RENAME_TABLE_QUERY = String.format(".rename table %s to %s", TABLE_NAME, NEW_TABLE_NAME);
    private final String CLEAR_SCHEMA_CACHE_QUERY = String.format(".clear table %s cache streamingingestion schema", TABLE_NAME);

    private ADXTableManager adxTableManager;

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
        adxTableManager = new ADXTableManager(kustoClient, clock);
    }

    //ensureThatADXResourcesExist checks if table and mapping exist for a given structure ID (part of AVRO schema)
    @Test
    public void testExistingTableAndMapping() throws DataClientException, DataServiceException {
        doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), anyString());
        adxTableManager.checkIfExists(getSimpleSampleSchema(), STRUCTURE_ID);

        verifyExistenceCheck();
    }

    @Test
    public void testExistanceCheckExceptions() throws DataClientException, DataServiceException {
        boolean[] tableExistsValues = {true, false};
        boolean[] mappingExistsValues = {true, false};

        for (boolean tableExists : tableExistsValues) {
            for (boolean mappingExists : mappingExistsValues) {
                reset(kustoClient);
                if (!tableExists) {
                    doThrow(DataClientException.class).when(kustoClient).execute(eq(DB_NAME), eq(String.format(KQLQueries.TABLE_EXISTS_QUERY, TABLE_NAME)));
                } else {
                    doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), eq(String.format(KQLQueries.TABLE_EXISTS_QUERY, TABLE_NAME)));
                }
                if (!mappingExists) {
                    doThrow(DataServiceException.class).when(kustoClient).execute(eq(DB_NAME), eq(String.format(KQLQueries.MAPPING_EXISTS_QUERY, TABLE_NAME, TABLE_NAME)));
                } else {
                    doReturn(getSampleResults(true)).when(kustoClient).execute(eq(DB_NAME), eq(String.format(KQLQueries.MAPPING_EXISTS_QUERY, TABLE_NAME, TABLE_NAME)));
                }

                adxTableManager.checkIfExists(getSimpleSampleSchema(), STRUCTURE_ID);

                if (!tableExists) {
                    verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_TABLE_CREATION_QUERY));
                    verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(BATCHING_POLICY_CREATION_QUERY));
                }

                if (!mappingExists) {
                    verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_MAPPING_CREATION_QUERY));
                }
            }
        }
    }

    @Test
    public void testTableCreationException() throws DataClientException, DataServiceException {
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), anyString());
        doThrow(DataClientException.class).when(kustoClient).execute(eq(DB_NAME), eq(SIMPLE_TABLE_CREATION_QUERY));

        //Exceptions when creating adx resources are wrapped as IngestionRuntimeException
        expectedException.expect(ADXClientException.class);
        adxTableManager.checkIfExists(getSimpleSampleSchema(), STRUCTURE_ID);
    }

    @Test
    public void testMappingCreationException() throws DataClientException, DataServiceException {
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), anyString());
        doThrow(DataClientException.class).when(kustoClient).execute(eq(DB_NAME), eq(SIMPLE_MAPPING_CREATION_QUERY));

        //Exceptions when creating adx resources are wrapped as IngestionRuntimeException
        expectedException.expect(ADXClientException.class);
        adxTableManager.checkIfExists(getSimpleSampleSchema(), STRUCTURE_ID);
    }

    @Test
    public void testPolicyCreationException() throws DataClientException, DataServiceException {
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), anyString());
        doThrow(DataClientException.class).when(kustoClient).execute(eq(DB_NAME), eq(BATCHING_POLICY_CREATION_QUERY));

        //Exceptions when creating adx resources are wrapped as IngestionRuntimeException
        expectedException.expect(ADXClientException.class);
        adxTableManager.checkIfExists(getSimpleSampleSchema(), STRUCTURE_ID);
    }

    @Test
    public void testSimpleTableAndMappingCreation() throws DataClientException, DataServiceException {
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), anyString());
        adxTableManager.checkIfExists(getSimpleSampleSchema(), STRUCTURE_ID);

        verifyExistenceCheck();

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_TABLE_CREATION_QUERY));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(BATCHING_POLICY_CREATION_QUERY));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_MAPPING_CREATION_QUERY));
    }

    @Test
    public void testStreamingPolicy() throws DataClientException, DataServiceException {
        environmentVariables.set(ADXConstants.INGESTION_TYPE_PROP, IngestionType.STREAMING.getValue());
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), anyString());
        adxTableManager.checkIfExists(getSimpleSampleSchema(), STRUCTURE_ID);
        environmentVariables.clear(ADXConstants.INGESTION_TYPE_PROP);

        verifyExistenceCheck();

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_TABLE_CREATION_QUERY));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(STREAMING_POLICY_CREATION_QUERY));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_MAPPING_CREATION_QUERY));
    }

    @Test
    public void testInvalidDataType() throws DataClientException, DataServiceException {
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), anyString());

        expectedException.expect(AvroIngestionException.class);
        adxTableManager.checkIfExists(getSampleSchemaWithInvalidDataType(), STRUCTURE_ID);
    }

    @Test
    public void testComplexTableAndMappingCreation() throws DataClientException, DataServiceException {
        doReturn(getSampleResults(false)).when(kustoClient).execute(eq(DB_NAME), anyString());
        adxTableManager.checkIfExists(getComplexSampleSchema(), STRUCTURE_ID);

        verifyExistenceCheck();

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(COMPLEX_TABLE_CREATION_QUERY));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(BATCHING_POLICY_CREATION_QUERY));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(COMPLEX_MAPPING_CREATION_QUERY));
    }

    @Test
    public void testUpdateTableAndMapping() throws DataClientException, DataServiceException {
        adxTableManager.updateTableAndMapping(getSimpleSampleSchema(), STRUCTURE_ID);

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_TABLE_UPDATE_QUERY));

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_MAPPING_CREATION_QUERY));
    }

    @Test
    public void testUpdateTableAndMappingException() throws DataClientException, DataServiceException {
        doThrow(DataClientException.class).when(kustoClient).execute(eq(DB_NAME), eq(SIMPLE_TABLE_UPDATE_QUERY));

        //Exceptions when creating adx resources are wrapped as ADXRuntimeException
        expectedException.expect(ADXClientException.class);
        adxTableManager.updateTableAndMapping(getSimpleSampleSchema(), STRUCTURE_ID);
    }

    @Test
    public void testUpdateColumn() throws DataClientException, DataServiceException {
        adxTableManager.updateColumn(STRUCTURE_ID, COLUMNN_NAME, COLUMNN_DATA_TYPE);

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(UPDATE_COLUMN_QUERY));
    }

    @Test
    public void testUpdateColumnException() throws DataClientException, DataServiceException {
        doThrow(DataClientException.class).when(kustoClient).execute(eq(DB_NAME), eq(UPDATE_COLUMN_QUERY));

        //Exceptions when creating adx resources are wrapped as ADXRuntimeException
        expectedException.expect(ADXClientException.class);
        adxTableManager.updateColumn(STRUCTURE_ID, COLUMNN_NAME, COLUMNN_DATA_TYPE);
    }

    @Test
    public void testDropTable() throws DataClientException, DataServiceException {
        adxTableManager.dropTable(STRUCTURE_ID);

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(DROP_TABLE_QUERY));
    }

    @Test
    public void testDropColumn() throws DataClientException, DataServiceException {
        adxTableManager.dropColumn(STRUCTURE_ID, COLUMNN_NAME, getSimpleSampleSchema());

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(DROP_COLUMN_QUERY));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_MAPPING_CREATION_QUERY));
    }

    @Test
    public void testDropColumnException() throws DataClientException, DataServiceException {
        doThrow(DataClientException.class).when(kustoClient).execute(eq(DB_NAME), eq(SIMPLE_MAPPING_CREATION_QUERY));

        //Exceptions when creating adx resources are wrapped as IngestionRuntimeException
        expectedException.expect(ADXClientException.class);
        adxTableManager.dropColumn(STRUCTURE_ID, COLUMNN_NAME, getSimpleSampleSchema());
    }

    @Test
    public void testRenameColumn() throws DataClientException, DataServiceException {
        doReturn(MOCK_MILLIS).when(clock).millis();
        adxTableManager.softDeleteColumn(STRUCTURE_ID, COLUMNN_NAME, getSimpleSampleSchema());

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(RENAME_COLUMN_QUERY));
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(SIMPLE_MAPPING_CREATION_QUERY));
    }

    @Test
    public void testRenameColumnException() throws DataClientException, DataServiceException {
        doThrow(DataClientException.class).when(kustoClient).execute(eq(DB_NAME), eq(SIMPLE_MAPPING_CREATION_QUERY));

        //Exceptions when creating adx resources are wrapped as IngestionRuntimeException
        expectedException.expect(ADXClientException.class);
        adxTableManager.softDeleteColumn(STRUCTURE_ID, COLUMNN_NAME, getSimpleSampleSchema());
    }

    @Test
    public void testRenameTable() throws DataClientException, DataServiceException {
        doReturn(MOCK_MILLIS).when(clock).millis();
        adxTableManager.softDeleteTable(STRUCTURE_ID);

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(RENAME_TABLE_QUERY));
    }

    @Test
    public void testClearSchemaCache() throws DataClientException, DataServiceException {
        doReturn(getSampleClearSchemaResults(true)).when(kustoClient).execute(eq(DB_NAME), anyString());
        adxTableManager.clearADXTableSchemaCache(STRUCTURE_ID);

        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(CLEAR_SCHEMA_CACHE_QUERY));
    }

    @Test
    public void testClearSchemaCacheRetry() throws DataClientException, DataServiceException {
        when(kustoClient.execute(eq(DB_NAME), anyString()))
                .thenReturn(getSampleClearSchemaResults(true, false))
                .thenReturn(getSampleClearSchemaResults(true, true));
        adxTableManager.clearADXTableSchemaCache(STRUCTURE_ID);

        verify(kustoClient, times(2)).execute(eq(DB_NAME), eq(CLEAR_SCHEMA_CACHE_QUERY));
    }

    @Test
    public void testUpdateDocStringSPI() throws DataClientException, DataServiceException {
        String getDocStringQuery = String.format(".show table %s cslschema", TABLE_NAME);
        String docStringQuery = String.format(".alter table %s docstring \"%s\"", TABLE_NAME, AvroConstants.GDPR_CATEGORY_SPI);

        doReturn(getSampleDocstringResults("")).when(kustoClient).execute(eq(DB_NAME), eq(getDocStringQuery));
        adxTableManager.updateGdprDocString(STRUCTURE_ID, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_GDPR_RELEVANT_SPI);


        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(docStringQuery));
    }

    @Test
    public void testUpdateDocStringPII() throws DataClientException, DataServiceException {
        String getDocStringQuery = String.format(".show table %s cslschema", TABLE_NAME);
        String docStringQuery = String.format(".alter table %s docstring \"%s\"", TABLE_NAME, AvroConstants.GDPR_CATEGORY_PII);

        doReturn(getSampleDocstringResults("")).when(kustoClient).execute(eq(DB_NAME), eq(getDocStringQuery));
        adxTableManager.updateGdprDocString(STRUCTURE_ID, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_GDPR_RELEVANT_PII);


        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(docStringQuery));
    }

    @Test
    public void testUpdateExistingDocString() throws DataClientException, DataServiceException {
        String oldDocString = "TESTMT";
        String getDocStringQuery = String.format(".show table %s cslschema", TABLE_NAME);
        String docStringQuery = String.format(".alter table %s docstring \"%s %s\"", TABLE_NAME, oldDocString,
                AvroConstants.GDPR_CATEGORY_SPI);

        doReturn(getSampleDocstringResults(oldDocString)).when(kustoClient).execute(eq(DB_NAME), eq(getDocStringQuery));
        adxTableManager.updateGdprDocString(STRUCTURE_ID, TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA_GDPR_RELEVANT_SPI);


        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(docStringQuery));
    }

    private void verifyExistenceCheck() throws DataClientException, DataServiceException {
        //table request executed on kusto client
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(String.format(KQLQueries.TABLE_EXISTS_QUERY, TABLE_NAME)));
        //mapping request executed on kusto client
        verify(kustoClient, times(1)).execute(eq(DB_NAME), eq(String.format(KQLQueries.MAPPING_EXISTS_QUERY, TABLE_NAME, TABLE_NAME)));
    }

    private String getSimpleSampleSchema() {
        return TestAVROSchemaConstants.SAMPLE_AVRO_SCHEMA;
    }

    private String getComplexSampleSchema() {
        return TestAVROSchemaConstants.SAMPLE_COMPLEX_AVRO_SCHEMA;
    }

    private String getSampleSchemaWithInvalidDataType() {
        return TestAVROSchemaConstants.SAMPLE_INVALID_AVRO_SCHEMA;
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

    private Results getSampleDocstringResults(String docString) {
        ArrayList<String> innerValues = new ArrayList<>();
        ArrayList<ArrayList<String>> values = new ArrayList<>();

        HashMap columnNameToIndex = new HashMap<>();
        columnNameToIndex.put(ADXConstants.DOC_STRING, 0);

        Results results = new Results(columnNameToIndex, null, values, null);

        innerValues.add(docString);
        values.add(innerValues);

        return results;
    }

    private Results getSampleClearSchemaResults(boolean... succeededEntries) {
        ArrayList<ArrayList<String>> values = new ArrayList<>();

        HashMap columnNameToIndex = new HashMap<>();
        columnNameToIndex.put(ADXConstants.STATUS, 0);

        Results results = new Results(columnNameToIndex, null, values, null);

        for (boolean succeeded : succeededEntries) {
            ArrayList<String> innerValues = new ArrayList<>();
            if (succeeded) {
                innerValues.add(ADXConstants.SUCCEEDED);
                values.add(innerValues);
            } else {
                innerValues.add("false");
                values.add(innerValues);
            }
        }

        return results;
    }
}