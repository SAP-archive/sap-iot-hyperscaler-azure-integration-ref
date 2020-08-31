package com.sap.iot.azure.ref.integration.commons.avro.logicaltypes;

public class CustomAvroException extends RuntimeException{

    public CustomAvroException(final String message) {
        super(message);
    }


    public CustomAvroException(final String s, final Throwable e) {
        super(s, e);
    }
}
