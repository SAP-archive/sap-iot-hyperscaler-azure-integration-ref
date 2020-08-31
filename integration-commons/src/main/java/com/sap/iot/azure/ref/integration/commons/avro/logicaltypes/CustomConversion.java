package com.sap.iot.azure.ref.integration.commons.avro.logicaltypes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContext;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.logging.Level;

public class CustomConversion {

    /**
     * Conversion class for String data Type
     */
    public static class StringConversion extends Conversion<String> {

        private static final String NSTRING = "nString";

        public Class<String> getConvertedType() {
            return String.class;
        }

        public String getLogicalTypeName() {
            return NSTRING;
        }

        @Override
        public String fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
            checkString(value.toString(), schema);
            return value.toString();
        }

        @Override
        public CharSequence toCharSequence(final String value, final Schema schema, final LogicalType type) {
            checkString(value, schema);
            return value;
        }

        private void checkString(final String value, final Schema schema) {
            if (value.length() > Integer.parseInt(schema.getObjectProp("max-length").toString())) {
                throw new IllegalArgumentException("Length of the String is greater than what is allowed in the schema");
            }
        }
    }


    /**
     * Conversion class for LargeString Data Type
     */
    public static class LargeStringConversion extends Conversion<String> {

        private static final String NLARGESTRING = "nLargeString";

        public Class<String> getConvertedType() {
            return String.class;
        }

        public String getLogicalTypeName() {
            return NLARGESTRING;
        }

        @Override
        public String fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
            checkLargeString(value.toString(), schema);
            return value.toString();
        }

        @Override
        public CharSequence toCharSequence(final String value, final Schema schema, final LogicalType type) {
            checkLargeString(value, schema);
            return value;
        }

        private void checkLargeString(final String value, final Schema schema) {
            if (value.length() > Integer.parseInt(schema.getObjectProp("max-length").toString())) {
                throw new IllegalArgumentException("Length of the LargeString is greater than what is allowed in the schema");
            }
        }

    }

    /**
     * Conversion class for ThingId and BPID data types
     */
    public static class UUIDConversion extends Conversion<String> {

        private static final String NUUID = "nUUID";
        static final String REGEX_UUID = "[0-9|A-F]{32}";

        public Class<String> getConvertedType() {
            return String.class;
        }

        public String getLogicalTypeName() {
            return NUUID;
        }

        @Override
        public String fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
            checkUUID(value.toString());
            return value.toString();

        }

        @Override
        public CharSequence toCharSequence(final String value, final Schema schema, final LogicalType type) {
            checkUUID(value);
            return value;
        }

        private void checkUUID(final String value) {
            if (!value.toString().matches(REGEX_UUID)) {
                throw new IllegalArgumentException("UUID does not match the given regrex");
            }
        }

    }


    /**
     * Conversion class for JSON data type
     */
    public static class JsonConversion extends Conversion<String> {

        private static final String NJson = "nJson";
        private static final ObjectMapper objectMapper = new ObjectMapper();

        public Class<String> getConvertedType() {
            return String.class;
        }

        public String getLogicalTypeName() {
            return NJson;
        }


        @Override
        public String fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type) {
            checkJson(value.toString(), schema);
            return value.toString();

        }

        @Override
        public CharSequence toCharSequence(final String value, final Schema schema, final LogicalType type) {
            checkJson(value, schema);
            return value;

        }

        private void checkJson(final String value, final Schema schema) {
            try {
                if (value.getBytes(StandardCharsets.UTF_16).length > Integer.parseInt(schema.getObjectProp("max-length").toString())) {
                    throw new IllegalArgumentException("Length of the JSON is greater than what is allowed in the schema");
                } else
                    objectMapper.readTree(value);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Not a Valid JSON", e);
            }
        }

    }

    /**
     * Conversion class for GeoJSON data type
     */

    public static class GeoJsonConversion extends Conversion<String>{

        private static final String NGeoJSON = "nGeoJSON";

        @Override
        public Class<String> getConvertedType() {
            return String.class;
        }

        public String getLogicalTypeName(){
            return NGeoJSON;
        }

        @Override
        public String fromCharSequence(final CharSequence value, final Schema schema, final LogicalType type){
            return value.toString();
        }

        @Override
        public CharSequence toCharSequence(final String value, final Schema schema, final LogicalType type){
            return value;
        }

    }


    /**
     * Conversion class for ByteArray Data Type
     */
    public static class ByteArrayConversion extends Conversion<byte[]> {

        private static final String nByte = "nByte";

        public Class<byte[]> getConvertedType() {
            return byte[].class;
        }

        public String getLogicalTypeName() {
            return nByte;
        }

        public ByteBuffer toBytes(final byte[] byteArray, final Schema schema, final LogicalType type) {
            checkByteArray(byteArray, schema);
            return ByteBuffer.wrap(byteArray);
        }

        public byte[] fromBytes(final ByteBuffer value, final Schema schema, final LogicalType type) {
            final byte[] byteArray = value.array();
            checkByteArray(byteArray, schema);
            return byteArray;
        }

        private void checkByteArray(final byte[] byteArray, final Schema schema) {
            if (byteArray.length > Integer.parseInt(schema.getObjectProp("max-length").toString())) {
                throw new IllegalArgumentException("Length of the ByteArray is greater than what is allowed in the schema");
            }
        }

    }

    /**
     * Conversion class for DateTime data Type
     */
    public static class TimeSecondsConversion extends Conversion<Instant> {

        private static final String timeSeconds = "timestamp-seconds";

        public Class<Instant> getConvertedType() {
            return Instant.class;
        }

        public String getLogicalTypeName() {
            return timeSeconds;
        }


       /* @Override
        public Long toLong(DateTime value, Schema schema, LogicalType type) {
            return value.getMillis()/1000;
        }

        @Override
        public DateTime fromLong(Long value, Schema schema, LogicalType type) {
            long mills=value*1000;
            return new DateTime(mills, DateTimeZone.UTC);
        }*/

        @Override
        public Long toLong(final Instant value, final Schema schema, final LogicalType type) {
            return value.getEpochSecond();
        }

        @Override
        public Instant fromLong(final Long value, final Schema schema, final LogicalType type) {
            return Instant.ofEpochSecond(value);
        }

    }

    /**
     * Conversion class for Decimal Data type
     */
    public static class DecimalConversion extends Conversion<BigDecimal> {

        private static final String nDecimal = "nDecimal";

        public Class<BigDecimal> getConvertedType() {
            return BigDecimal.class;
        }

        public String getLogicalTypeName() {
            return nDecimal;
        }

        @Override
        public BigDecimal fromBytes(final ByteBuffer value, final Schema schema, final LogicalType type) {

            final byte[] bytes = value.get(new byte[value.remaining()]).array();
            final byte[] actualValue = Arrays.copyOfRange(bytes, 1, bytes.length);
            final BigDecimal bigDecimal = new BigDecimal(new BigInteger(actualValue), bytes[0]);
            checkDecimal(bigDecimal, schema);
            return bigDecimal;


        }

        @Override
        public ByteBuffer toBytes(final BigDecimal value, final Schema schema, final LogicalType type)  {
            checkDecimal(value, schema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write((byte)value.scale());

            try {
                out.write(value.unscaledValue().toByteArray());
            } catch (IOException e) {
                InvocationContext.getLogger().log(Level.SEVERE, "Writing of value to the OutputStream Failed. ", e);
                throw new CustomAvroException("Writing of value to the OutputStream Failed. ",e);
            }


            return ByteBuffer.wrap(out.toByteArray());
        }

        private void checkDecimal(final BigDecimal value, final Schema schema) {
            final int scale = Integer.parseInt(schema.getObjectProp("scale").toString());
            final int precision = Integer.parseInt(schema.getObjectProp("precision").toString());
            if (scale < value.scale()) {
                throw new IllegalArgumentException("Scale cant be greater than the specified scale in schema");
            }
            if (precision < value.precision()) {
                throw new IllegalArgumentException("Precision cant be greater than the specified precision in schema");
            }

        }

    }


    /**
     * Conversion class for Long Data type with is compatible with datetime
     */
    public static class LongTimeStampConversion extends Conversion<Long> {

        private static final String nTimestamp = "nTimestamp";

        public Class<Long> getConvertedType() {
            return Long.class;
        }

        public String getLogicalTypeName() {
            return nTimestamp;
        }

        @Override
        public Long fromLong(final Long value, final Schema schema, final LogicalType type) {
            if(value < 0 ) throw new IllegalArgumentException("Invalid timestamp value.");
            try {
                new DateTime(value, DateTimeZone.UTC);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid timestamp value.",e);
            }
            return value;
        }

        @Override
        public Long toLong(final Long value, final Schema schema, final LogicalType type) {

            if(value < 0 ) throw new IllegalArgumentException("Invalid timestamp value.");

            try {
                new DateTime(value, DateTimeZone.UTC);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid timestamp value.",e);
            }
            return value;
        }

    }

    /**
     * Custom Conversion class for Decimal Data type
     */
    public static class TimeMillisConversion extends Conversion<Long> {

        private static final String timestampMillis = "timestamp-millis";

        public Class<Long> getConvertedType() {
            return Long.class;
        }

        public String getLogicalTypeName() {
            return timestampMillis;
        }

        @Override
        public Long fromLong(final Long value, final Schema schema, final LogicalType type) {

            if (value < 0) throw new IllegalArgumentException("Invalid timestamp-millis value.");
            return value;
        }

        @Override
        public Long toLong(final Long value, final Schema schema, final LogicalType type) {

            if (value < 0) throw new IllegalArgumentException("Invalid timestamp-millis value.");
            return value;
        }
    }

}
