package com.sap.iot.azure.ref.delete.model.cloudEvents;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.Attributes;
import io.cloudevents.CloudEvent;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.extensions.InMemoryFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SapIoTAbstractionExtension {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String correlationId;

    public static class Format implements ExtensionFormat {

        public static final String SAP_IOT_ABSTRACTION_EXTENSION = "comSapIoTAbstractionExtension";
        public static final String SAP_IOT_ABSTRACTION_COR_KEY = "correlationId";

        private final InMemoryFormat inMemoryFormat;
        private final Map<String, String> transport = new HashMap<>();

        public Format (SapIoTAbstractionExtension extension) {
            inMemoryFormat = InMemoryFormat.of(SAP_IOT_ABSTRACTION_EXTENSION, extension, SapIoTAbstractionExtension.class);
            transport.put(SAP_IOT_ABSTRACTION_COR_KEY, extension.getCorrelationId());
        }

        @Override
        public InMemoryFormat memory() {
            return inMemoryFormat;
        }

        @Override
        public Map<String, String> transport() {
            return transport;
        }
    }

    public static <A extends Attributes, T> SapIoTAbstractionExtension getExtension(CloudEvent<A, T> cloudEvent) {
        return objectMapper.convertValue(cloudEvent.getExtensions().get(Format.SAP_IOT_ABSTRACTION_EXTENSION), SapIoTAbstractionExtension.class);
    }
}
