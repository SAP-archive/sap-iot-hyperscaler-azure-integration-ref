package com.sap.iot.azure.ref.integration.commons.model.mapping.api.mapping;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.sap.iot.azure.ref.integration.commons.model.mapping.cache.PropertyMapping;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.List;

public class PropertyMappingsJsonDeserializer extends JsonDeserializer<List<PropertyMapping>> {
    @Override
    public List<PropertyMapping> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        InnerPropertyMappings innerPropertyMappings = jp.readValueAs(InnerPropertyMappings.class);

        return innerPropertyMappings.results;
    }

    @Getter
    @Setter
    private static class InnerPropertyMappings {
        private List<PropertyMapping> results;
    }
}
