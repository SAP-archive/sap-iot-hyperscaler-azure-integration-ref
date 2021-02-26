package com.sap.iot.azure.ref.integration.commons.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.Results;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import com.sap.iot.azure.ref.integration.commons.exception.ADXClientException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.logging.Level;

public class ADXDataManager {
    private final String ADX_DATABASE_NAME = System.getenv(ADXConstants.ADX_DATABASE_NAME_PROP);
    private Client kustoClient;
    private Client kustoIngestionClient;

    public ADXDataManager() {
        this(KustoClientFactory.getClient(), KustoClientFactory.getIngestionClient());
    }

    ADXDataManager(Client kustoClient, Client kustoIngestionClient) {
        this.kustoClient = kustoClient;
        this.kustoIngestionClient = kustoIngestionClient;
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
     * @param columnName,  Column Name. Used for forming the data exists query.
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
     * Execute soft delete time series request.
     * Returns the operation ID of the delete query.
     *
     * @param request used for generating time series deletion query
     * @return Operation ID of delete query
     * @throws ADXClientException exception in adx interaction
     */
    public String deleteTimeSeries(DeleteInfo request) throws ADXClientException {
        String structureId = request.getStructureId();
        Results results = executeQueryForStructureId(KQLQueries.getDeleteTimeSeriesQuery(request),
                structureId);

        if (results.getValues().isEmpty() || results.getValues().get(0).isEmpty() || results.getValues().get(0).get(0) == null || results.getValues().get(0).get(0).isEmpty()) {
            throw new ADXClientException("Delete TimeSeries Query did not return an operation ID",
                    IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId), false);
        } else {
            return results.getValues().get(0).get(0);
        }
    }

    /**
     * Execute purge time series request.
     * Returns the operation ID of the purge query.
     *
     * @param requests used for generating purge query
     * @return Operation ID of purge query
     * @throws ADXClientException is thrown if no operation ID is included in the query result
     */
    public String purgeTimeSeries(String structureId, List<DeleteInfo> requests) throws ADXClientException {
        try {
            Results results = kustoIngestionClient.execute((KQLQueries.getPurgeTimeSeriesQuery(structureId, requests, ADX_DATABASE_NAME)));
            if (results.getValues().isEmpty() || results.getValues().get(0).isEmpty() || results.getValues().get(0).get(0) == null || results.getValues().get(0).get(0).isEmpty()) {
                throw new ADXClientException("Purge Query did not return an operation ID",
                        IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId), false);
            } else {
                return results.getValues().get(0).get(0);
            }
        } catch (DataServiceException | DataClientException e) {
            throw getADXExceptionWithStructureId(e, structureId);
        }
    }

    /**
     * Fetches the status of a delete operation.
     * Returns the operation status as {@link DeleteOperationStatus}.
     *
     * @param operationId used for forming operation status query
     * @param structureId used for forming exception identifier
     * @return delete operation status for given operation id
     * @throws ADXClientException exception while interacting with ADX
     */
    public DeleteOperationStatus getDeleteOperationStatus(String operationId, String structureId) throws ADXClientException {
        try {
            Results results = kustoClient.execute(KQLQueries.getDeleteOperationStatus(operationId));
            if(!isEmpty(results)) {
                return DeleteOperationStatus.ofType(results.getValues().get(0).get(results.getColumnNameToIndex().get("State")));
            } else {
                throw new ADXClientException("Delete time series query did not return an operation status",
                        IdentifierUtil.getIdentifier(CommonConstants.OPERATION_ID, operationId, CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId), false);
            }
        } catch (DataServiceException | DataClientException ex) {
            throw getADXExceptionWithStructureId(ex, structureId);
        }
    }

    /**
     * Fetches the status of a purge operation.
     * Returns the operation status as {@link DeleteOperationStatus}.
     * Uses the ingestion client for executing the kusto query.
     *
     * @param operationId used for forming operation status query
     * @param structureId used for forming exception identifier
     * @return delete operation status for given operation id
     * @throws ADXClientException exception while interacting with ADX
     */
    public DeleteOperationStatus getPurgeOperationStatus(String operationId, String structureId) throws ADXClientException {
        try {
            Results results = kustoIngestionClient.execute(KQLQueries.getPurgeOperationStatus(operationId));
            if(!isEmpty(results)) {
                return DeleteOperationStatus.ofType(results.getValues().get(0).get(results.getColumnNameToIndex().get("State")));
            } else {
                throw new ADXClientException("Purge time series query did not return an operation status",
                        IdentifierUtil.getIdentifier(CommonConstants.OPERATION_ID, operationId, CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId), false);
            }
        } catch (DataServiceException | DataClientException ex) {
            throw getADXExceptionWithStructureId(ex, structureId);
        }
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

    boolean isEmpty(Results results) {
        return CollectionUtils.isEmpty(results.getValues()) //check results values is null/empty
                || CollectionUtils.isEmpty(results.getValues().get(0)) //check is first entry for results is null/empty
                || StringUtils.isEmpty(results.getValues().get(0).get(results.getColumnNameToIndex().get("State"))); //check if State for result is null/empty
    }
}