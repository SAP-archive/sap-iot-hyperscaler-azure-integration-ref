package com.sap.iot.azure.ref.integration.commons.adx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.kusto.data.ClientImpl;
import com.microsoft.azure.kusto.data.Results;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import com.sap.iot.azure.ref.integration.commons.avro.AvroHelper;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.exception.AvroIngestionException;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.Clock;
import java.util.Map;
import java.util.logging.Level;

public class ADXTableManager {
    private final String ADX_DATABASE_NAME = System.getenv(ADXConstants.ADX_DATABASE_NAME_PROP);
    private ClientImpl kustoClient;
    private Clock clock;


    public ADXTableManager() {
        this(KustoClientFactory.getClient(), Clock.systemUTC());
    }

    ADXTableManager(ClientImpl kustoClient, Clock clock) {
        this.kustoClient = kustoClient;
        this.clock = clock;
    }

    /**
     * Checks if an ADX Table and Mapping exists for a given structure.
     * If the table or mapping does not exist, it is created with an ingestion policy. The table and mapping names are constructed of the structure ID and
     * {@link ADXConstants#TABLE_PREFIX}.
     * The column information is retrieved from the AVRO schema, which is passed as parameter.
     * For executing the kusto queries, this class uses the {@link ClientImpl Azure Kusto Client}.
     * The ADX database name is fetched from {@link ADXConstants#ADX_DATABASE_NAME_PROP}.
     *
     * @param schemaString, AVRO schema. Used for constructing the column information.
     * @param structureId,  Structure ID. Used for constructing the table and mapping name.
     * @throws AvroIngestionException exception in parsing avro schema
     * @throws ADXClientException     exception in adx interaction
     */
    public void checkIfExists(String schemaString, String structureId) throws AvroIngestionException, ADXClientException {
        //check table existence
        boolean tableExists = doesQueryReturnResults(KQLQueries.getTableExistsQuery(structureId));
        //check mapping existence
        boolean mappingExists = doesQueryReturnResults(KQLQueries.getMappingExistsQuery(structureId));

        //if not correct, create new one
        if (!tableExists || !mappingExists) {
            try {
                //fetch columns
                Map<String, String> columnInfo = AvroHelper.getColumnInfo(structureId, schemaString);

                if (!tableExists) {
                    //create table
                    createTable(structureId, columnInfo);
                    createPolicy(structureId);
                }
                if (!mappingExists) {
                    //create mapping
                    createMapping(structureId, columnInfo);
                }
            } catch (SchemaParseException ex) {
                throw new AvroIngestionException("error in parsing Avro Schema", ex, IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY,
                        structureId));
            } catch (DataServiceException | DataClientException e) {
                throw getADXExceptionWithStructureId(e, structureId);
            } catch (JsonProcessingException e) {
                throw IoTRuntimeException.wrapNonTransient(IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId),
                        CommonErrorType.JSON_PROCESSING_ERROR, "Error in preparing json mapping for ADX Ingestion", e);
            }
        }
    }

    /**
     * Updates ADX Table and Mapping for a given structure and AVRO schema.
     * For updating the table, an alter-merge operation is used. For updating the mapping, an create-or-alter operation is used.
     *
     * @param schemaString, AVRO schema. Used for constructing the column information.
     * @param structureId,  Structure ID. Used for constructing the table and mapping name.
     * @throws ADXClientException exception in adx interaction
     */
    public void updateTableAndMapping(String schemaString, String structureId) throws ADXClientException {
        //fetch columns
        Map<String, String> columnInfo = AvroHelper.getColumnInfo(structureId, schemaString);
        try {
            kustoClient.execute(ADX_DATABASE_NAME, KQLQueries.getUpdateTableQuery(structureId, columnInfo));
            InvocationContext.getLogger().log(Level.INFO, String.format("Updated ADX table for structure ID '%s'", structureId));
            createMapping(structureId, columnInfo);
        } catch (JsonProcessingException e) {
            throw IoTRuntimeException.wrapNonTransient(IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId),
                    CommonErrorType.JSON_PROCESSING_ERROR, "Error in preparing json mapping for Table Update", e);
        } catch (DataServiceException | DataClientException e) {
            throw getADXExceptionWithStructureId(e, structureId);
        }
    }

    /**
     * Drops table for a given structure ID.
     *
     * @param structureId structure ID for which the table should be dropped
     * @throws ADXClientException exception in adx interaction
     */
    public void dropTable(String structureId) {
        executeQueryForStructureId(KQLQueries.getDropTableQuery(structureId), structureId);
        InvocationContext.getLogger().log(Level.INFO, String.format("Dropped ADX table for structure ID '%s'", structureId));
    }

    /**
     * Updates the datatype of a given ADX column.
     *
     * @param structureId, Structure ID. Used for forming the update query.
     * @param columnName,  Column which should be updated
     * @param dataType,    new data type of the column
     * @throws ADXClientException exception in adx interaction
     */
    public void updateColumn(String structureId, String columnName, String dataType) throws ADXClientException {
        executeQueryForStructureId(KQLQueries.getColumnUpdateQuery(structureId, columnName, dataType), structureId);
        InvocationContext.getLogger().log(Level.INFO, String.format("Updated column '%s' for structure ID '%s'", columnName, structureId));
    }

    /**
     * Drops a given ADX column.
     *
     * @param structureId, Structure ID. Used for forming the query.
     * @param columnName,  Column which should be dropped
     * @throws ADXClientException exception in adx interaction
     */
    public void dropColumn(String structureId, String columnName) throws ADXClientException {
        executeQueryForStructureId(KQLQueries.getDropColumnQuery(structureId, columnName), structureId);
        InvocationContext.getLogger().log(Level.INFO, String.format("Dropped column '%s' for structure ID '%s'", columnName, structureId));
    }

    /**
     * Check if data exists for a given Structure ID.
     *
     * @param structureId, Structure ID. Used for forming the data exists query.
     * @throws ADXClientException exception in adx interaction
     */
    public boolean dataExists(String structureId) throws ADXClientException {
        Results results = executeQueryForStructureId(KQLQueries.getDataExistsQuery(structureId), structureId);

        if (results.getValues().isEmpty()) {
            InvocationContext.getLogger().log(Level.INFO, String.format("No data found for structure ID '%s'", structureId));
            return false;
        } else {
            InvocationContext.getLogger().log(Level.INFO, String.format("Data found for structure ID '%s'", structureId));
            return true;
        }
    }

    /**
     * Check if data exists for a given Structure ID and column.
     *
     * @param structureId, Structure ID. Used for forming the data exists query.
     * @param columnName, Column Name. Used for forming the data exists query.
     * @throws ADXClientException exception in adx interaction
     */
    public boolean dataExistsForColumn(String structureId, String columnName) throws ADXClientException {
        Results results = executeQueryForStructureId(KQLQueries.getColumnDataExistsQuery(structureId, columnName), structureId);

        if (results.getValues().isEmpty()) {
            InvocationContext.getLogger().log(Level.INFO, String.format("No data found for column '%s' and structure ID '%s'", columnName, structureId));
            return false;
        } else {
            InvocationContext.getLogger().log(Level.INFO, String.format("Data found for column '%s' and structure ID '%s'", columnName, structureId));
            return true;
        }
    }

    /**
     * Execute soft delete request for a given structure ID and column name.
     *
     * @param structureId   used for generating the table name
     * @param columnName  name of the column
     * @throws ADXClientException exception in adx interaction
     */
    public void softDeleteColumn(String structureId, String columnName) throws ADXClientException {
        executeQueryForStructureId(KQLQueries.getColumnSoftDeleteQuery(structureId, columnName, clock), structureId);
    }

    /**
     * Execute soft delete request for a given structure ID.
     *
     * @param structureId   used for generating the table name
     * @throws ADXClientException exception in adx interaction
     */
    public void softDeleteTable(String structureId) throws ADXClientException {
        executeQueryForStructureId(KQLQueries.getTableSoftDeleteQuery(structureId, clock), structureId);
    }

    private void createTable(String structureId, Map<String, String> columnInfo) throws DataClientException, DataServiceException {
        kustoClient.execute(ADX_DATABASE_NAME, KQLQueries.getCreateTableQuery(structureId, columnInfo));
        InvocationContext.getLogger().log(Level.INFO, String.format("Created ADX table for structure ID '%s'", structureId));
    }

    private void createMapping(String structureId, Map<String, String> columnInfo) throws RuntimeException, JsonProcessingException, DataClientException, DataServiceException {
        kustoClient.execute(ADX_DATABASE_NAME, KQLQueries.getCreateMappingQuery(structureId, columnInfo));
        InvocationContext.getLogger().log(Level.INFO, String.format("Created mapping for structure ID '%s'", structureId));
    }

    private void createPolicy(String structureId) throws DataClientException, DataServiceException {
        kustoClient.execute(ADX_DATABASE_NAME, KQLQueries.getCreatePolicyQuery(structureId));
        InvocationContext.getLogger().log(Level.INFO, String.format("Created policy for structure ID '%s'", structureId));
    }

    private boolean doesQueryReturnResults(String query) {
        boolean returnsResults = false;

        try {
            //if nothing is found, exception is thrown
            Results results = kustoClient.execute(ADX_DATABASE_NAME, query);
            if (results.getValues() != null && !results.getValues().isEmpty()) {
                returnsResults = true;
            }
        } catch (DataServiceException | DataClientException ex) {
            InvocationContext.getContext().getLogger().log(Level.INFO, "Query did not return results.");
        }

        return returnsResults;
    }

    private Results executeQueryForStructureId(String query, String structureId) throws ADXClientException {
        try {
            return kustoClient.execute(ADX_DATABASE_NAME, query);
        } catch (DataServiceException | DataClientException e) {
            throw getADXExceptionWithStructureId(e, structureId);
        }
    }

    private ADXClientException getADXExceptionWithStructureId(Exception e, String structureId) {
        int dataWebExIndex = ExceptionUtils.indexOfType(e, DataWebException.class);
        boolean isTransient = false;

        // check if exception has DataWebException in the cause chain
        if (dataWebExIndex >= 0) {
            DataWebException dataWebException = (DataWebException) ExceptionUtils.getThrowableList(e).get(dataWebExIndex);

            if (dataWebException.getHttpResponse().getStatusLine().getStatusCode() >= 500) {
                // permanent ex
                isTransient = true;
            }
        }

        return new ADXClientException("Exception in accessing ADX artifacts", e, IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY,
                structureId), isTransient);
    }
}