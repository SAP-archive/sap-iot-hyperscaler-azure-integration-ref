package com.sap.iot.azure.ref.integration.commons.avro.logicaltypes;


import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;

public class RegisterService {
    private static final GenericData genericData = new GenericData();

    /**
     * * @return Generic Data object which can be used to write Avro Messages.
     */
    public static GenericData initializeCustomTypes() {

        LogicalTypes.register("nString", new LogicalTypes.LogicalTypeFactory() {
            public LogicalType fromSchema(final Schema schema) {
                return CustomLogicalTypes.stringConversionWithAnnotation(Integer.parseInt(schema.getObjectProp("max-length").toString()),
                        Boolean.valueOf(schema.getObjectProp("has-annotation").toString()));
            }
        });

        LogicalTypes.register("nString", new LogicalTypes.LogicalTypeFactory() {
            public LogicalType fromSchema(final Schema schema) {
                return CustomLogicalTypes.stringConversionWithOutAnnotation(Integer.parseInt(schema.getObjectProp("max-length").toString()) );
            }
        });

        LogicalTypes.register("nJson", new LogicalTypes.LogicalTypeFactory() {
            public LogicalType fromSchema(final Schema schema) {
                return CustomLogicalTypes.jsonConversion(Integer.parseInt(schema.getObjectProp("max-length").toString()));
            }
        });

        LogicalTypes.register("nByte", new LogicalTypes.LogicalTypeFactory() {
            public LogicalType fromSchema(final Schema schema) {
                return CustomLogicalTypes.byteArrayConversion(Integer.parseInt(schema.getObjectProp("max-length").toString()));
            }
        });

        LogicalTypes.register("timestamp-seconds", new LogicalTypes.LogicalTypeFactory() {
            public LogicalType fromSchema(final Schema schema) {
                return CustomLogicalTypes.timeSecsConversion();
            }
        });

        LogicalTypes.register("nLargeString", new LogicalTypes.LogicalTypeFactory() {
            public LogicalType fromSchema(final Schema schema) {
                return CustomLogicalTypes.largeStringConversion(Integer.parseInt(schema.getObjectProp("max-length").toString()));
            }
        });

        LogicalTypes.register("nUUID", new LogicalTypes.LogicalTypeFactory() {
            public LogicalType fromSchema(final Schema schema) {
                return CustomLogicalTypes.uuidConversion(Integer.parseInt(schema.getObjectProp("fix-length").toString()));
            }
        });

        LogicalTypes.register("nDecimal", new LogicalTypes.LogicalTypeFactory() {
            public LogicalType fromSchema(final Schema schema) {
                return CustomLogicalTypes.decimalConversion(Integer.parseInt(schema.getObjectProp("scale").toString()), Integer.parseInt(schema.getObjectProp("precision").toString()));
            }
        });

        LogicalTypes.register("nTimestamp", new LogicalTypes.LogicalTypeFactory() {
            public LogicalType fromSchema(final Schema schema) {
                return CustomLogicalTypes.timestampConversion();
            }
        });

        LogicalTypes.register("nGeoJSON", new LogicalTypes.LogicalTypeFactory(){
            public LogicalType fromSchema(final Schema schema){
                return CustomLogicalTypes.geoJSONConversion();
            }
        });

        LogicalTypes.register("timestamp-millis", new LogicalTypes.LogicalTypeFactory(){
            public LogicalType fromSchema(final Schema schema){
                return CustomLogicalTypes.timestampMillisConversion();
            }
        });

        genericData.addLogicalTypeConversion(new CustomConversion.TimeSecondsConversion());
        genericData.addLogicalTypeConversion(new CustomConversion.UUIDConversion());
        genericData.addLogicalTypeConversion(new CustomConversion.StringConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
        genericData.addLogicalTypeConversion(new CustomConversion.TimeMillisConversion());
        genericData.addLogicalTypeConversion(new CustomConversion.ByteArrayConversion());
        genericData.addLogicalTypeConversion(new CustomConversion.DecimalConversion());
        genericData.addLogicalTypeConversion(new CustomConversion.JsonConversion());
        genericData.addLogicalTypeConversion(new CustomConversion.LargeStringConversion());
        genericData.addLogicalTypeConversion(new CustomConversion.LongTimeStampConversion());
        genericData.addLogicalTypeConversion(new CustomConversion.GeoJsonConversion());

        return genericData;
    }
}
