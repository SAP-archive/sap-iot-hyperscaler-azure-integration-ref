package com.sap.iot.azure.ref.device.management.model.cloudevents;

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
public class SAPIoTAbstractionExtension {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private String transactionId;
    private String sequenceNumber;

    public static class Format implements ExtensionFormat {

        public static final String SAP_IOT_ABSTRACTION_EXTENSION = "comSapIoTAbstractionExtension";
        public static final String SAP_IOT_ABSTRACTION_TX_KEY = "transactionId";
        public static final String SAP_IOT_ABSTRACTION_TX_SEQUENCE = "sequenceNumber";

        private final InMemoryFormat inMemoryFormat;
        private final Map<String, String> transport = new HashMap<>();

        public Format (SAPIoTAbstractionExtension extension) {
           inMemoryFormat = InMemoryFormat.of(SAP_IOT_ABSTRACTION_EXTENSION, extension, SAPIoTAbstractionExtension.class);
           transport.put(SAP_IOT_ABSTRACTION_TX_KEY, extension.getTransactionId());
           transport.put(SAP_IOT_ABSTRACTION_TX_SEQUENCE, extension.getSequenceNumber());
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

    public static <A extends Attributes, T> SAPIoTAbstractionExtension getExtension(CloudEvent<A, T> cloudEvent) {
        return objectMapper.convertValue(cloudEvent.getExtensions().get(Format.SAP_IOT_ABSTRACTION_EXTENSION), SAPIoTAbstractionExtension.class);
    }
}
