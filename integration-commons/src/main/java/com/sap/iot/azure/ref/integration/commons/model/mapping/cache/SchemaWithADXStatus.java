package com.sap.iot.azure.ref.integration.commons.model.mapping.cache;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SchemaWithADXStatus {

    private String avroSchema; // Avro schema corresponding to this structure

    // based on this value, the adx table sync will be executed during ingestion. if it still fails, ingestion for that message / group of message will fail
    private boolean adxSync = false; // initial value - either adx sync is not initiated or has failed.

    // status of ADX is unknown (initial)
    public SchemaWithADXStatus(String avroSchema) {
        this.avroSchema = avroSchema;
    }

    public SchemaWithADXStatus withADXSyncStatus(boolean status) {
        this.adxSync = status;
        return this;
    }
}