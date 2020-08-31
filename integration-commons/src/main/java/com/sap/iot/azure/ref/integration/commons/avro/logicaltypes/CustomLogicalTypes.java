package com.sap.iot.azure.ref.integration.commons.avro.logicaltypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class CustomLogicalTypes {

    public static CustomString stringConversionWithAnnotation(final int maxLen,final boolean hasAnnotation) {
        return new CustomString(maxLen,hasAnnotation);
    }

    public static CustomString stringConversionWithOutAnnotation(final int maxLen) {
        return new CustomString(maxLen);
    }


    public static TimestampSeconds timeSecsConversion() {
        return new TimestampSeconds();
    }

    public static CustomDecimal decimalConversion(final int scale, final int precision) {
        return new CustomDecimal(scale, precision);
    }

    public static CustomByteArray byteArrayConversion(final int maxLen) {
        return new CustomByteArray(maxLen);
    }

    public static CustomLargeString largeStringConversion(final int maxLen) {
        return new CustomLargeString(maxLen);
    }

    public static CustomJson jsonConversion(final int maxLen) {
        return new CustomJson(maxLen);
    }

    public static CustomUUID uuidConversion(final int fixLen) {
        return new CustomUUID(fixLen);
    }

    public static CustomTimestamp timestampConversion() {
        return new CustomTimestamp();
    }

    public static CustomGeoJSON geoJSONConversion(){
        return new CustomGeoJSON();
    }

    public static CustomTimestampMillis timestampMillisConversion() {
        return new CustomTimestampMillis();
    }

    /**
     * Class for Custom Logical Type "nString"
     */
    public static class CustomString extends LogicalType {
        private static final String nSTRING = "nString";
        private final int maxLength;
        private final boolean hasAnnotation;

        private CustomString(final int maxLength)
        {
            super(nSTRING);
            this.maxLength = maxLength;
            this.hasAnnotation=false;
        }

        private CustomString(final int maxLength,final boolean hasAnnotation) {
            super(nSTRING);
            this.maxLength = maxLength;
            this.hasAnnotation = hasAnnotation;
        }

        @Override
        public void validate(final Schema schema) {
            super.validate(schema);

            if (schema.getType() != Schema.Type.STRING) {
                throw new IllegalArgumentException("nSTRING can only be backed by String type");
            }

        }

        @Override
        public Schema addToSchema(final Schema schema) {
            super.addToSchema(schema);
            schema.addProp("max-length", maxLength);
            schema.addProp("has-annotation",hasAnnotation);
            return schema;
        }
    }


    /**
     * Class for Custom Logical Type "nJson"
     */
    public static class CustomJson extends LogicalType {
        private static final String nJson = "nJson";
        private final int maxLength;


        private CustomJson(final int maxLength) {
            super(nJson);
            this.maxLength = maxLength;
        }

        @Override
        public void validate(final Schema schema) {
            super.validate(schema);

            if (schema.getType() != Schema.Type.STRING) {
                throw new IllegalArgumentException("nJSon can only be backed by String type");
            }

        }

        @Override
        public Schema addToSchema(final Schema schema) {
            super.addToSchema(schema);
            schema.addProp("max-length", maxLength);
            return schema;
        }
    }

    /**
     * class for Custom Logical Type "timestamp-seconds"
     */
    public static class TimestampSeconds extends LogicalType {
        private static final String timestampSeconds = "timestamp-seconds";

        private TimestampSeconds() {
            super(timestampSeconds);

        }

        @Override
        public void validate(final Schema schema) {
            super.validate(schema);

            if (schema.getType() != Schema.Type.LONG) {
                throw new IllegalArgumentException("timestampSeconds can only be backed by Long type");
            }

        }

        @Override
        public Schema addToSchema(final Schema schema) {
            super.addToSchema(schema);
            return schema;
        }
    }

    /**
     * class for Custom Logical Type "nUUID"
     */
    public static class CustomUUID extends LogicalType {
        private static final String nUUID = "nUUID";
        private final int fixedLength;


        private CustomUUID(final int fixedLength) {
            super(nUUID);
            this.fixedLength = fixedLength;
        }

        @Override
        public void validate(final Schema schema) {
            super.validate(schema);

            if (schema.getType() != Schema.Type.STRING) {
                throw new IllegalArgumentException("nUUID can only be backed by String type");
            }

        }

        @Override
        public Schema addToSchema(final Schema schema) {
            super.addToSchema(schema);
            schema.addProp("fix-length", fixedLength);
            return schema;
        }
    }

    /**
     * class for Custom Logical Type "nDecimal"
     */
    public static class CustomDecimal extends LogicalType {
        private static final String nDecimal = "nDecimal";
        private final int scale;
        private final int precision;


        private CustomDecimal(final int scale, final int precision) {
            super(nDecimal);
            this.scale = scale;
            this.precision = precision;
        }

        @Override
        public void validate(final Schema schema) {
            super.validate(schema);

            if (schema.getType() != Schema.Type.BYTES) {
                throw new IllegalArgumentException("nDecimal can only be backed by Bytes type");
            }

        }

        @Override
        public Schema addToSchema(final Schema schema) {
            super.addToSchema(schema);
            schema.addProp("scale", scale);
            schema.addProp("precision", precision);
            return schema;
        }
    }

    /**
     * class for Custom Logical Type "nLargeString"
     */
    public static class CustomLargeString extends LogicalType {
        private static final String nLARGESTRING = "nLargeString";
        private final int maxLength;

        private CustomLargeString(final int maxLength) {
            super(nLARGESTRING);
            this.maxLength = maxLength;
        }

        @Override
        public void validate(final Schema schema) {
            super.validate(schema);

            if (schema.getType() != Schema.Type.STRING) {
                throw new IllegalArgumentException("nLARGESTRING can only be backed by string type");
            }

        }

        @Override
        public Schema addToSchema(final Schema schema) {
            super.addToSchema(schema);
            schema.addProp("max-length", maxLength);
            return schema;
        }
    }

    /**
     * class for Custom Logical Type "nByte"
     */
    public static class CustomByteArray extends LogicalType {
        private static final String nBYTEARRAY = "nByte";
        private final int maxLength;


        private CustomByteArray(final int maxLength) {
            super(nBYTEARRAY);
            this.maxLength = maxLength;
        }

        @Override
        public void validate(final Schema schema) {
            super.validate(schema);

            if (schema.getType() != Schema.Type.BYTES) {
                throw new IllegalArgumentException("nBYTE can only be backed by BYTE type");
            }

        }

        @Override
        public Schema addToSchema(final Schema schema) {
            super.addToSchema(schema);
            schema.addProp("max-length", maxLength);
            return schema;
        }
    }

    /**
     * Class for Custom Logical Type "nTimestamp"
     */
    public static class CustomTimestamp extends LogicalType {
        private static final String nTIMESTAMP = "nTimestamp";

        private CustomTimestamp() {
            super(nTIMESTAMP);
        }

        @Override
        public void validate(final Schema schema) {
            super.validate(schema);

            if (schema.getType() != Schema.Type.LONG) {
                throw new IllegalArgumentException("nTimestamp can only be backed by Long type");
            }

        }

        @Override
        public Schema addToSchema(final Schema schema) {
            super.addToSchema(schema);
            return schema;
        }
    }


    /**
     * Class for Custom Logical Type "nGeoJson"
     */

    public static class CustomGeoJSON extends LogicalType {
        private static final String nJson = "nGeoJSON";


        private CustomGeoJSON() {
            super(nJson);

        }

        @Override
        public void validate(final Schema schema) {
            super.validate(schema);

            if(schema.getType() != Schema.Type.STRING){
                throw new IllegalArgumentException("nGeoJSon can only be backed by String type");
            }
        }

        @Override
        public Schema addToSchema(final Schema schema) {
            super.addToSchema(schema);
            return schema;
        }
    }

    /**
     * class for Custom Logical Type "timestamp-millis"
     */
    public static class CustomTimestampMillis extends LogicalType {

        private static final String timestampMillis = "timestamp-millis";

        private CustomTimestampMillis() {

            super(timestampMillis);
        }

        @Override
        public void validate(final Schema schema) {

            super.validate(schema);

            if (schema.getType() != Schema.Type.LONG) {
                throw new IllegalArgumentException("timestampMillis can only be backed by Long type");
            }
        }

        @Override
        public Schema addToSchema(final Schema schema) {

            super.addToSchema(schema);
            return schema;
        }
    }

}
