package com.sap.iot.azure.ref.integration.commons.mapping;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import com.sap.iot.azure.ref.integration.commons.constants.CommonConstants;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.MappingLookupException;
import com.sap.iot.azure.ref.integration.commons.mapping.api.AsyncHttpClientFactory;
import com.sap.iot.azure.ref.integration.commons.mapping.token.TenantTokenCache;
import com.sap.iot.azure.ref.integration.commons.mapping.util.HttpResponseUtil;
import com.sap.iot.azure.ref.integration.commons.model.mapping.api.assignment.AssignmentEndpointResponse;
import com.sap.iot.azure.ref.integration.commons.model.mapping.api.mapping.MappingEndpointResponse;
import com.sap.iot.azure.ref.integration.commons.model.mapping.api.tags.Tag;
import com.sap.iot.azure.ref.integration.commons.model.mapping.api.tags.TagEndpointResponse;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMappingInfo;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.SensorAssignment;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class MappingServiceLookup {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final AsyncHttpClient asyncHttpClient;
    private final TenantTokenCache tenantTokenCache;

    public MappingServiceLookup() {
        this(new AsyncHttpClientFactory().getAsyncHttpClientWitHResponseFilter(), new TenantTokenCache());
    }

    @VisibleForTesting
    MappingServiceLookup(AsyncHttpClient asyncHttpClient, TenantTokenCache tenantTokenCache) {
        this.asyncHttpClient = asyncHttpClient;
        this.tenantTokenCache = tenantTokenCache;
    }

    /**
     * Fetch device mapping information for a given sensor and virtual capability ID.
     * The device mapping information includes the source ID, tags and mapping ID.
     *
     * @param sensorId,            required for fetching tags and assignment info
     * @param virtualCapabilityId, required for fetching tags
     * @return {@link SensorInfo} including device mapping information
     * @throws MappingLookupException in case device info lookup fails
     */

    SensorInfo getSensorInfo(String sensorId, String virtualCapabilityId, Optional<SensorAssignment> sensorAssignmentCache) throws MappingLookupException {
        SensorInfo sensorInfo = SensorInfo.builder()
                .sensorId(sensorId)
                .virtualCapabilityId(virtualCapabilityId)
                .build();

        //get source ID & tags from tags endpoint
        TagEndpointResponse tagEndpointResponse = getTags(sensorId, virtualCapabilityId);
        AssignmentEndpointResponse assignmentEndpointResponse;

        //get mappingId, assignmentId and objectId from assignment endpoint depending on the sensorAssignmentCache details, if it is present
        if (sensorAssignmentCache.isPresent()) {
            //retrieving assignment information from the cache for a given sensorId
            String mappingId = sensorAssignmentCache.get().getMappingId();
            String assignmentId = sensorAssignmentCache.get().getAssignmentId();
            assignmentEndpointResponse = AssignmentEndpointResponse.builder()
                    .assignmentId(assignmentId)
                    .mappingId(mappingId)
                    .build();
        } else {
            //retrieving assignment information from the API endpoint for a given sensorId
            assignmentEndpointResponse = getAssignment(sensorId);
        }
        sensorInfo = addSensorInfo(sensorInfo, tagEndpointResponse, assignmentEndpointResponse);
        return sensorInfo;
    }

    /**
     * Fetches a list of property mapping information for a given mapping ID.
     * Every property mapping information includes a list of property mappings for a mapping and structure ID.
     *
     * @param mappingId, for which the list property mapping information is fetched
     * @return {@link List} of {@link PropertyMappingInfo} objects
     * @throws MappingLookupException exception in mapping lookup
     */
    List<PropertyMappingInfo> getPropertyMappingInfos(String mappingId) throws MappingLookupException {
        try {
            return getMappings(mappingId).stream()
                    .map(mappingEndpointResponse ->
                            PropertyMappingInfo.builder()
                                    .mappingId(mappingId)
                                    .structureId(mappingEndpointResponse.getStructureId())
                                    .virtualCapabilityId(mappingEndpointResponse.getCapabilityId())
                                    .propertyMappings(mappingEndpointResponse.getPropertyMappings())
                                    .build())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new MappingLookupException("Property Mapping Info Lookup failed",
                    IdentifierUtil.getIdentifier(CommonConstants.MAPPING_ID_PROPERTY_KEY, mappingId), false);
        } catch (ExecutionException e) {
            throw new MappingLookupException("Property Mapping Info Lookup failed",
                    IdentifierUtil.getIdentifier(CommonConstants.MAPPING_ID_PROPERTY_KEY, mappingId), true);
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            throw new MappingLookupException("Property Mapping Info Lookup failed due to an interruption",
                    IdentifierUtil.getIdentifier(CommonConstants.MAPPING_ID_PROPERTY_KEY, mappingId), true);
        }
    }

    /**
     * Fetches the AVRO schema for a given structure ID as string.
     *
     * @param structureId, for which the AVRO schema is fetched
     * @return AVRO schema as string
     */
    public String getSchemaInfo(String structureId) throws MappingLookupException {
        try {
            return getSchema(structureId);

        } catch (ExecutionException e) {
            throw new MappingLookupException("Schema Info Lookup failed", IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY,
                    structureId), true);
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            throw new MappingLookupException("Schema Info Lookup failed due to an interruption",
                    IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId), true);
        }
    }

    private SensorInfo addSensorInfo(SensorInfo sensorInfo, TagEndpointResponse tagEndpointResponse, AssignmentEndpointResponse assignmentEndpointResponse) {
        sensorInfo.setSourceId(tagEndpointResponse.getSourceId());
        sensorInfo.setStructureId(tagEndpointResponse.getStructureId());
        sensorInfo.setMappingId(assignmentEndpointResponse.getMappingId());
        sensorInfo.setTags(convertAPIToModelTags(tagEndpointResponse.getTags()));

        return sensorInfo;
    }

    private List<com.sap.iot.azure.ref.integration.commons.model.mapping.cache.Tag> convertAPIToModelTags(List<Tag> apiTags) {
        return apiTags.stream().map(apiTag -> com.sap.iot.azure.ref.integration.commons.model.mapping.cache.Tag.builder().tagSemantic(apiTag.getTagSemantic()).tagValue(apiTag.getTagValue()).build()).collect(Collectors.toList());
    }

    private TagEndpointResponse getTags(String sensorId, String virtualCapabilityId) throws MappingLookupException {
        try {
            Response tagsResponse = asyncHttpClient.prepareGet(MappingServiceConstants.TAGS_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, sensorId).replace(MappingServiceConstants.VIRTUAL_CAPABILITY_ID_PLACEHOLDER, virtualCapabilityId))
                    .setHeader(HttpHeaders.AUTHORIZATION, MappingServiceConstants.BEARER_TOKEN_PREFIX + tenantTokenCache.getToken())
                    .execute()
                    .get();


            HttpResponseUtil.validateHttpResponse(tagsResponse, "tags service");
            String tagsValue = tagsResponse.getResponseBody();

            return objectMapper.readValue(tagsValue, TagEndpointResponse.class);

        } catch (JsonParseException | JsonMappingException e) {
            throw new MappingLookupException("Invalid Tags returned", IdentifierUtil.getIdentifier(CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId,
                    CommonConstants.VIRTUAL_CAPABILITY_ID_PROPERTY_KEY, virtualCapabilityId), false);
        } catch (IOException | ExecutionException e) {
            throw new MappingLookupException("Unable to fetch Tags", IdentifierUtil.getIdentifier(CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId,
                    CommonConstants.VIRTUAL_CAPABILITY_ID_PROPERTY_KEY, virtualCapabilityId), true);
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            throw new MappingLookupException("Unable to fetch Tags due to an interruption",
                    IdentifierUtil.getIdentifier(CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId, CommonConstants.VIRTUAL_CAPABILITY_ID_PROPERTY_KEY,
                            virtualCapabilityId), true);
        }
    }

    private AssignmentEndpointResponse getAssignment(String sensorId) throws MappingLookupException {
        try {
            Response assignmentResponse = asyncHttpClient.prepareGet(MappingServiceConstants.ASSIGNMENT_ENDPOINT.replace(MappingServiceConstants.SENSOR_ID_PLACEHOLDER, sensorId))
                    .setHeader(HttpHeaders.AUTHORIZATION, MappingServiceConstants.BEARER_TOKEN_PREFIX + tenantTokenCache.getToken())
                    .execute()
                    .get();

            HttpResponseUtil.validateHttpResponse(assignmentResponse, "assignment service");

            JsonNode assignment = objectMapper.readTree(assignmentResponse.getResponseBody()).get("d").get("results").get(0);

            return objectMapper.readValue(assignment.toString(), AssignmentEndpointResponse.class);
        } catch (JsonParseException | JsonMappingException e) {
            throw new MappingLookupException("Invalid Assignment returned", IdentifierUtil.getIdentifier(CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId,
                    CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId), false);
        } catch (IOException | ExecutionException e) {
            throw new MappingLookupException("Unable to fetch Assignment", IdentifierUtil.getIdentifier(CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId,
                    CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId), true);
        } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
            throw new MappingLookupException("Unable to fetch Assignment due to an interruption",
                    IdentifierUtil.getIdentifier(CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId, CommonConstants.SENSOR_ID_PROPERTY_KEY, sensorId), true);
        }
    }

    private List<MappingEndpointResponse> getMappings(String mappingId) throws ExecutionException, InterruptedException, IOException {
        Response mappingsResponse = asyncHttpClient.prepareGet(MappingServiceConstants.MAPPING_ENDPOINT.replace(MappingServiceConstants.MAPPING_ID_PLACEHOLDER, mappingId))
                .setHeader(HttpHeaders.AUTHORIZATION, MappingServiceConstants.BEARER_TOKEN_PREFIX + tenantTokenCache.getToken())
                .execute()
                .get();

        HttpResponseUtil.validateHttpResponse(mappingsResponse, "mapping service");

        return objectMapper.readValue(objectMapper.readTree(mappingsResponse.getResponseBody()).get(MappingServiceConstants.API_RESPONSE_BODY_KEY_D).get(MappingServiceConstants.API_RESPONSE_BODY_KEY_MEASURES).get(MappingServiceConstants.API_RESPONSE_BODY_KEY_RESULTS).toString(), new TypeReference<List<MappingEndpointResponse>>() {
        });
    }

    private String getSchema(String structureId) throws ExecutionException, InterruptedException {
        Response schemaResponse = asyncHttpClient.prepareGet(MappingServiceConstants.SCHEMA_ENDPOINT.replace(MappingServiceConstants.STRUCTURE_ID_PLACEHOLDER, structureId))
                .setHeader(HttpHeaders.AUTHORIZATION, MappingServiceConstants.BEARER_TOKEN_PREFIX + tenantTokenCache.getToken())
                .execute()
                .get();
        String schemaString = schemaResponse.getResponseBody();
        HttpResponseUtil.validateHttpResponse(schemaResponse, "schema service");

        try {
            new Schema.Parser().parse(schemaString);
        } catch (SchemaParseException e) {
            throw new MappingLookupException("Invalid Schema returned", IdentifierUtil.getIdentifier(CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId,
                    CommonConstants.STRUCTURE_ID_PROPERTY_KEY, structureId), false);
        }

        return schemaString;
    }
}