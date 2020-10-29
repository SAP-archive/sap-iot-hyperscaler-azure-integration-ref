package com.sap.iot.azure.ref.integration.commons.adx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.model.timeseries.delete.DeleteInfo;
import com.sap.iot.azure.ref.integration.commons.util.EnvUtils;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class KQLQueries {
    static final String TABLE_EXISTS_QUERY = ".show table %s details";
    static final String MAPPING_EXISTS_QUERY = ".show table %s ingestion json mapping '%s'";
    private static final String TABLE_QUERY = ".%s table %s (%s)";
    private static final String DROP_TABLE_QUERY = ".drop table %s ifexists";
    static final String CREATE_POLICY_QUERY = ".alter table %s policy ingestionbatching @'{\"MaximumBatchingTimeSpan\":\"00:01:00\", " +
            "\"MaximumNumberOfItems\": %s, \"MaximumRawDataSizeMB\": %s}'";
    private static final String UPDATE_COLUMN_QUERY = ".alter column ['%s'].['%s'] type=%s";
    private static final String DROP_COLUMN_QUERY = ".drop column %s . %s";
    private static final String CREATE_MAPPING_QUERY = ".create-or-alter table %s ingestion json mapping '%s' '%s'";
    private static final String DATA_EXISTS_QUERY = "%s | take 1";
    private static final String COLUMN_DATA_EXISTS_QUERY = "%s | project %s | where isnotempty(%s) | take 1";
    private static final String RENAME_TABLE_QUERY = ".rename table %s to %s";
    private static final String RENAME_COLUMN_QUERY = ".rename column %s . %s to %s";
    private static final String SOURCE_ID_FILTER_PLACEHOLDER = "{SOURCE_ID_FILTER}";
    private static final String FROM_INCLUSIVE_PLACEHOLDER = "{FROM_INCLUSIVE}";
    private static final String TO_INCLUSIVE_PLACEHOLDER = "{TO_INCLUSIVE}";
    private static final String SOURCE_ID_FILTER_CLAUSE = "and sourceId in (%s) ";
    private static final String DELETE_TIMESERIES_QUERY = ".set-or-append async %s <| %s " +
            "| where (_time >" + FROM_INCLUSIVE_PLACEHOLDER + " datetime(\"%s\") " +
            "and _time <" + TO_INCLUSIVE_PLACEHOLDER + " datetime(\"%s\")) " +
            SOURCE_ID_FILTER_PLACEHOLDER +
            "and _enqueued_time < datetime(\"%s\") " +
            "| where _isDeleted != true " +
            "| extend _enqueued_time = now(), _isDeleted = true";
    private static final String KEY_VALUE_PLACEHOLDER = "%s: %s";
    private static final String MAPPING_COLUMN_KEY = "column";
    private static final String MAPPING_PATH_KEY = "path";
    private static final String MAPPING_PATH_ROOT_PLACEHOLDER = "$.";
    private static final String MEASUREMENTS_MAPPING_PATH = ADXConstants.MEASUREMENTS_PROPERTY_KEY + ".";
    private static final String COMMA_SEPARATOR = ", ";
    private static final String CREATE_OPERATOR = "create-merge";
    private static final String ALTER_OPERATOR = "alter";
    private static final String ALTER_MERGE_OPERATOR = ALTER_OPERATOR + "-merge";
    static final String DEFAULT_MAX_NUMBER_OF_ITEMS = "100000";
    static final String DEFAULT_MAX_RAW_DATA_SIZE = "1024";
    private static final String MAX_NUMBER_OF_ITEMS_PROP = "max-number-of-items";
    private static final String MAX_RAW_DATA_SIZE_PROP = "max-number-of-items";
    private static final String MAX_NUMBER_OF_ITEMS = EnvUtils.getEnv(MAX_NUMBER_OF_ITEMS_PROP, DEFAULT_MAX_NUMBER_OF_ITEMS);
    private static final String MAX_RAW_DATA_SIZE = EnvUtils.getEnv(MAX_RAW_DATA_SIZE_PROP, DEFAULT_MAX_RAW_DATA_SIZE);
    public static final String SOFT_DELETE_SUFFIX = "_D_%s";
    private static final String DELETE_OPERATION_STATUS = ".show operations %s";
    public static final String INCLUSIVE_OPERATOR = "=";
    public static final String IN_CONDITION_SPLITTER = ", ";

    private static final ObjectMapper mapper = new ObjectMapper();

    static String getTableExistsQuery(String structureId) {
        return String.format(TABLE_EXISTS_QUERY, getTableName(structureId));
    }

    static String getMappingExistsQuery(String structureId) {
        String tableName = getTableName(structureId);

        return String.format(MAPPING_EXISTS_QUERY, tableName, tableName);
    }

    static String getCreateTableQuery(String structureId, Map<String, String> columnInfo) {
        return String.format(TABLE_QUERY, CREATE_OPERATOR, getTableName(structureId),
                getColumnString(columnInfo));
    }

    static String getUpdateTableQuery(String structureId, Map<String, String> columnInfo) {
        return String.format(TABLE_QUERY, ALTER_MERGE_OPERATOR, getTableName(structureId), getColumnString(columnInfo));
    }

    static String getDropTableQuery(String structureId) {
        return String.format(DROP_TABLE_QUERY, getTableName(structureId));
    }

    static String getCreateMappingQuery(String structureId, Map<String, String> columnInfo) throws JsonProcessingException {
        return String.format(CREATE_MAPPING_QUERY, getTableName(structureId), getTableName(structureId),
                createMappingString(columnInfo));
    }

    static String getCreatePolicyQuery(String structureId) {
        return String.format(CREATE_POLICY_QUERY, getTableName(structureId), MAX_NUMBER_OF_ITEMS, MAX_RAW_DATA_SIZE);
    }

    static String getColumnUpdateQuery(String structureId, String column, String datatype) {
        return String.format(UPDATE_COLUMN_QUERY, getTableName(structureId), column, datatype);
    }

    static String getDropColumnQuery(String structureId, String column) {
        return String.format(DROP_COLUMN_QUERY, getTableName(structureId), column);
    }

    static String getColumnSoftDeleteQuery(String structureId, String columnName, Clock clock) {
        return String.format(RENAME_COLUMN_QUERY, getTableName(structureId), columnName, columnName + getSoftDeleteSuffix(clock));
    }

    static String getTableSoftDeleteQuery(String structureId, Clock clock) {
        String tableName = getTableName(structureId);

        return String.format(RENAME_TABLE_QUERY, tableName, tableName + getSoftDeleteSuffix(clock));
    }

    static String getDataExistsQuery(String structureId) {
        return String.format(DATA_EXISTS_QUERY, getTableName(structureId));
    }

    static String getColumnDataExistsQuery(String structureId, String columnName) {
        return String.format(COLUMN_DATA_EXISTS_QUERY, getTableName(structureId), columnName, columnName);
    }

    static String getDeleteTimeSeriesQuery(DeleteInfo request) {
        String tableName = getTableName(request.getStructureId());
        String query = DELETE_TIMESERIES_QUERY;
        query = addInclusiveInfo(query, request.getFromTimestampInclusive(), request.getToTimestampInclusive());
        query = addSourceIdFilter(query, request.getSourceIds());

        query = String.format(query, tableName, tableName, request.getFromTimestamp(), request.getToTimestamp(), request.getIngestionTimestamp());

        return query;
    }

    private static String getTableName(String structureId) {
        return ADXConstants.TABLE_PREFIX + structureId;
    }

    private static String getColumnString(Map<String, String> columnInfo) {
        AtomicReference<String> sResult = new AtomicReference<>("");
        AtomicBoolean first = new AtomicBoolean(true);

        columnInfo.forEach((key, value) -> {
            if (!first.get()) {
                sResult.set(sResult + COMMA_SEPARATOR);
            }
            first.set(false);
            sResult.set(sResult + String.format(KEY_VALUE_PLACEHOLDER, key, value));
        });

        return sResult.get();
    }

    private static String createMappingString(Map<String, String> columnInfo) throws JsonProcessingException {
        List<Map<String, String>> mappingInfo = new ArrayList<>();

        columnInfo.forEach((key, value) -> {
            Map<String, String> entry = new HashMap<>();
            entry.put(MAPPING_COLUMN_KEY, key);
            entry.put(MAPPING_PATH_KEY, getMappingPath(key));
            mappingInfo.add(entry);
        });

        return mapper.writeValueAsString(mappingInfo);
    }

    private static String getMappingPath(String property) {
        String path = property;
        String[] rootLevelProperties = new String[]{
                CommonConstants.SOURCE_ID_PROPERTY_KEY,
                CommonConstants.TIMESTAMP_PROPERTY_KEY,
                CommonConstants.ENQUEUED_TIME_PROPERTY_VALUE,
                CommonConstants.DELETED_PROPERTY_KEY
        };

        if (path.equals(CommonConstants.DELETED_PROPERTY_KEY)) {
            //Since isDeleted is a constant value, we do not add the path root
            return CommonConstants.DELETED_PROPERTY_VALUE;
        } else if (path.equals(CommonConstants.ENQUEUED_TIME_PROPERTY_KEY)) {
            //The path of the enqueued time is different from the column name
            path = CommonConstants.ENQUEUED_TIME_PROPERTY_VALUE;
        }

        if (!Arrays.asList(rootLevelProperties).contains(path)) {
            path = MEASUREMENTS_MAPPING_PATH + path;
        }

        return MAPPING_PATH_ROOT_PLACEHOLDER + path;
    }

    private static String getSoftDeleteSuffix(Clock clock) {
        //We add a timestamp to the suffix to avoid name collisions with future changes
        return String.format(SOFT_DELETE_SUFFIX, clock.millis());
    }

    static String getDeleteOperationStatus(String operationId) {
        return String.format(DELETE_OPERATION_STATUS, operationId);
    }

    private static String addInclusiveInfo(String input, boolean fromTimestampInclusive, boolean toTimestampInclusive) {
        String fromInclusive = fromTimestampInclusive ? INCLUSIVE_OPERATOR : "";
        String toInclusive = toTimestampInclusive ? INCLUSIVE_OPERATOR : "";

        input = input.replace(FROM_INCLUSIVE_PLACEHOLDER, fromInclusive);
        input = input.replace(TO_INCLUSIVE_PLACEHOLDER, toInclusive);

        return input;
    }

    private static String addSourceIdFilter(String input, List<String> sourceIds) {
        boolean sourceIdFilterExists = sourceIds != null && sourceIds.size() > 0;
        String sourceIdFilter = sourceIdFilterExists ? getSourceIdFilterString(sourceIds) : "";

        input = input.replace(SOURCE_ID_FILTER_PLACEHOLDER, sourceIdFilter);

        return input;
    }

    private static String getSourceIdFilterString(List<String> sourceIds) {
        return String.format(SOURCE_ID_FILTER_CLAUSE, sourceIds.stream()
                .map((sourceId) -> {return "\"" + sourceId + "\"";})
                .collect(Collectors.joining(IN_CONDITION_SPLITTER)));
    }
}
