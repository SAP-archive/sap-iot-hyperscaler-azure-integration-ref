package com.sap.iot.azure.ref.integration.commons.avro.logicaltypes;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Random;

public class CustomLogicalTypesTest {

    private GenericData genericData;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        genericData = RegisterService.initializeCustomTypes();
    }

    @Test
    public void testStringLogicalTypeNegative() {

        CustomLogicalTypes.CustomString customString = CustomLogicalTypes.stringConversionWithAnnotation(5, true);
        Schema schema = SchemaBuilder.builder().stringType();
        customString.validate(schema);
        Schema nStringSchema = customString.addToSchema(schema);

        Conversion stringConversion = genericData.getConversionFor(customString);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Length of the String is greater than what is allowed in the schema");
        stringConversion.fromCharSequence("abcdef", nStringSchema, customString);
    }

    @Test
    public void testStringLogicalType() {

        CustomLogicalTypes.CustomString customString = CustomLogicalTypes.stringConversionWithOutAnnotation(5);
        Schema schema = SchemaBuilder.builder().stringType();
        customString.validate(schema);
        Schema nStringSchema = customString.addToSchema(schema);

        Conversion stringConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nStringSchema));
        stringConversion.fromCharSequence("abcd", nStringSchema, customString);
        CharSequence seq = stringConversion.toCharSequence("test", nStringSchema, customString);
        Assert.assertEquals(seq, "test");
    }


    @Test
    public void testLargeStringLogicalTypeNegative() {

        CustomLogicalTypes.CustomLargeString customLargeString = CustomLogicalTypes.largeStringConversion(5);
        Schema schema = SchemaBuilder.builder().stringType();
        customLargeString.validate(schema);
        Schema nLargeStringSchema = customLargeString.addToSchema(schema);

        Conversion largeStringtringConversion = genericData.getConversionFor(customLargeString);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Length of the LargeString is greater than what is allowed in the schema");
        largeStringtringConversion.fromCharSequence("abcdef", nLargeStringSchema, customLargeString);
    }

    @Test
    public void testLargeStringLogicalType() {

        CustomLogicalTypes.CustomLargeString customLargeString = CustomLogicalTypes.largeStringConversion(5);
        Schema schema = SchemaBuilder.builder().stringType();
        customLargeString.validate(schema);
        Schema nLargeStringSchema = customLargeString.addToSchema(schema);

        Conversion largeStringConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nLargeStringSchema));
        largeStringConversion.fromCharSequence("abcd", nLargeStringSchema, customLargeString);
        CharSequence seq = largeStringConversion.toCharSequence("test", nLargeStringSchema, customLargeString);
        Assert.assertEquals(seq, "test");
    }


    @Test
    public void testUUIDLogicalTypeNegative() {

        CustomLogicalTypes.CustomUUID customUUID = CustomLogicalTypes.uuidConversion(32);
        Schema schema = SchemaBuilder.builder().stringType();
        customUUID.validate(schema);
        Schema nUUIDSchema = customUUID.addToSchema(schema);

        Conversion uuidConversion = genericData.getConversionFor(customUUID);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("UUID does not match the given regrex");
        uuidConversion.fromCharSequence("79132D40F9E4474", nUUIDSchema, customUUID);
    }

    @Test
    public void testUUIDLogicalType() {

        CustomLogicalTypes.CustomUUID customUUID = CustomLogicalTypes.uuidConversion(32);
        Schema schema = SchemaBuilder.builder().stringType();
        customUUID.validate(schema);
        Schema nUUIDSchema = customUUID.addToSchema(schema);

        Conversion uuidConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nUUIDSchema));
        uuidConversion.fromCharSequence("79132D40F9E4474AB21B15B2FB744D9C", nUUIDSchema, customUUID);
        CharSequence seq = uuidConversion.toCharSequence("79132D40F9E4474AB21B15B2FB744D9C", nUUIDSchema, customUUID);
        Assert.assertEquals(seq, "79132D40F9E4474AB21B15B2FB744D9C");
    }


    @Test
    public void testDecimalLogicalTypeNegative() throws IOException {

        CustomLogicalTypes.CustomDecimal customDecimal = CustomLogicalTypes.decimalConversion(2, 4);
        Schema schema = SchemaBuilder.builder().bytesType();
        customDecimal.validate(schema);
        Schema nDecimalSchema = customDecimal.addToSchema(schema);

        Conversion decimalConversion = genericData.getConversionFor(customDecimal);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(2);
        byte[] byteArray = new BigDecimal(new BigInteger(20, new Random()), 2, new MathContext(7)).unscaledValue().toByteArray();
        out.write(byteArray);
        ByteBuffer bbuf = ByteBuffer.wrap(out.toByteArray());
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Precision cant be greater than the specified precision in schema");
        decimalConversion.fromBytes(bbuf, nDecimalSchema, customDecimal);
    }

    @Test
    public void testDecimalLogicalType() {

        CustomLogicalTypes.CustomDecimal customDecimal = CustomLogicalTypes.decimalConversion(2, 4);
        Schema schema = SchemaBuilder.builder().bytesType();
        customDecimal.validate(schema);
        Schema nDecimalSchema = customDecimal.addToSchema(schema);

        Conversion decimalConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nDecimalSchema));
        BigDecimal bigDecimal = new BigDecimal(new BigInteger(8, new Random()), 2, new MathContext(4));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write((byte) 2);
        try {
            out.write(bigDecimal.unscaledValue().toByteArray());
        } catch (IOException e) {
            throw new CustomAvroException("Writing of value to the OutputStream Failed. ", e);
        }
        ByteBuffer byteBuffer = decimalConversion.toBytes(bigDecimal, nDecimalSchema, customDecimal);
        Assert.assertEquals(byteBuffer, ByteBuffer.wrap(out.toByteArray()));
    }


    @Test
    public void testTimeSecondsLogicalTypeNegative() {

        CustomLogicalTypes.TimestampSeconds timestampSeconds = CustomLogicalTypes.timeSecsConversion();
        Schema schema = SchemaBuilder.builder().stringType();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("timestampSeconds can only be backed by Long type");
        timestampSeconds.validate(schema);
    }

    @Test
    public void testTimeSecondsLogicalType() {

        CustomLogicalTypes.TimestampSeconds timestampSeconds = CustomLogicalTypes.timeSecsConversion();
        Schema schema = SchemaBuilder.builder().longType();
        timestampSeconds.validate(schema);
        Schema nTimeSchema = timestampSeconds.addToSchema(schema);

        Conversion timeConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nTimeSchema));
        timeConversion.fromLong(new Long(45), nTimeSchema, timestampSeconds);
        Instant timeIns = Instant.EPOCH.plusSeconds(5);
        Assert.assertEquals(new Long(5), timeConversion.toLong(timeIns, nTimeSchema, timestampSeconds));
    }


    @Test
    public void testJsonLogicalTypeNegative() {

        CustomLogicalTypes.CustomJson customJson = CustomLogicalTypes.jsonConversion(60);
        Schema schema = SchemaBuilder.builder().stringType();
        customJson.validate(schema);
        Schema nJsonSchema = customJson.addToSchema(schema);
        Conversion jsonConversion = genericData.getConversionFor(customJson);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Not a Valid JSON");
        jsonConversion.fromCharSequence("TestJson", nJsonSchema, customJson);
    }

    @Test
    public void testJsonLogicalType() {

        CustomLogicalTypes.CustomJson customJson = CustomLogicalTypes.jsonConversion(60);
        Schema schema = SchemaBuilder.builder().stringType();
        customJson.validate(schema);
        Schema nJsonSchema = customJson.addToSchema(schema);

        Conversion jsonConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nJsonSchema));
        jsonConversion.fromCharSequence("{\"name\":\"dummy\"}", nJsonSchema, customJson);
        Assert.assertEquals("{\"name\":\"dummy\"}", jsonConversion.toCharSequence("{\"name\":\"dummy\"}", nJsonSchema, customJson));
    }


    @Test
    public void testByteArrayLogicalTypeNegative() {

        CustomLogicalTypes.CustomByteArray customByte = CustomLogicalTypes.byteArrayConversion(5);
        Schema schema = SchemaBuilder.builder().bytesType();
        customByte.validate(schema);
        Schema nByteSchema = customByte.addToSchema(schema);
        // LogicalTypes.fromSchema(nByteSchema);
        ByteBuffer bbuf = ByteBuffer.allocate(6);
        bbuf.put("abcdef".getBytes());
        Conversion byteConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nByteSchema));
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Length of the ByteArray is greater than what is allowed in the schema");
        byteConversion.fromBytes(bbuf, nByteSchema, customByte);
    }

    @Test
    public void testByteArrayLogicalType() {

        CustomLogicalTypes.CustomByteArray customByte = CustomLogicalTypes.byteArrayConversion(5);
        Schema schema = SchemaBuilder.builder().bytesType();
        customByte.validate(schema);
        Schema nByteSchema = customByte.addToSchema(schema);
        ByteBuffer bbuf = ByteBuffer.allocate(4);
        bbuf.put("abcd".getBytes());
        Conversion byteConversion = genericData.getConversionFor(customByte);
        byteConversion.fromBytes(bbuf, nByteSchema, customByte);
        Assert.assertEquals(byteConversion.toBytes("abcd".getBytes(), nByteSchema, customByte).array().length, 4);
    }

    @Test
    public void testTimestampLogicalType() {

        CustomLogicalTypes.CustomTimestamp customTimestamp = CustomLogicalTypes.timestampConversion();
        Schema schema = SchemaBuilder.builder().longType();
        customTimestamp.validate(schema);
        Schema nTimestampSchema = customTimestamp.addToSchema(schema);

        Long value = 1530183698480L;
        Conversion timestampConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nTimestampSchema));
        timestampConversion.fromLong(value, nTimestampSchema, customTimestamp);

        Assert.assertEquals(timestampConversion.toLong(value, nTimestampSchema, customTimestamp), value);
    }

    @Test
    public void testTimestampLogicalTypeWithZero() {

        CustomLogicalTypes.CustomTimestamp customTimestamp = CustomLogicalTypes.timestampConversion();
        Schema schema = SchemaBuilder.builder().longType();
        customTimestamp.validate(schema);
        Schema nTimestampSchema = customTimestamp.addToSchema(schema);

        Long value = 0L;
        Conversion timestampConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nTimestampSchema));
        timestampConversion.fromLong(value, nTimestampSchema, customTimestamp);

        Assert.assertEquals(timestampConversion.toLong(value, nTimestampSchema, customTimestamp), value);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testTimestampLogicalTypeWithNegative() {

        CustomLogicalTypes.CustomTimestamp customTimestamp = CustomLogicalTypes.timestampConversion();
        Schema schema = SchemaBuilder.builder().longType();
        customTimestamp.validate(schema);
        Schema nTimestampSchema = customTimestamp.addToSchema(schema);

        Long value = -1530L;
        Conversion timestampConversion = genericData.getConversionFor(LogicalTypes.fromSchema(nTimestampSchema));
        timestampConversion.fromLong(value, nTimestampSchema, customTimestamp);
    }

    @Test
    public void testGeoJSONLogicalTypePositive() {

        CustomLogicalTypes.CustomGeoJSON customGeoJSON = CustomLogicalTypes.geoJSONConversion();
        Schema schema = SchemaBuilder.builder().stringType();
        customGeoJSON.validate(schema);

        Schema ngeoJSONSchema = customGeoJSON.addToSchema(schema);

        Conversion geoJSONConversion = genericData.getConversionFor(LogicalTypes.fromSchema(ngeoJSONSchema));
        geoJSONConversion.fromCharSequence("{\"type\":\"Point\",\"coordinates\":[78.47444,17.37528]}", ngeoJSONSchema, customGeoJSON);

        CharSequence seq = geoJSONConversion.toCharSequence("{\"type\":\"Point\",\"coordinates\":[78.47444,17.37528]}", ngeoJSONSchema, customGeoJSON);
        Assert.assertEquals("{\"type\":\"Point\",\"coordinates\":[78.47444,17.37528]}", seq);

    }

    @Test
    public void testTimestampMillisLogicalType() {

        CustomLogicalTypes.CustomTimestampMillis customTimestampMillis = CustomLogicalTypes.timestampMillisConversion();
        Schema schema = SchemaBuilder.builder().longType();
        customTimestampMillis.validate(schema);
        Schema timestampMillisSchema = customTimestampMillis.addToSchema(schema);

        Long value = 1530183698480L;
        Conversion timestampConversion = genericData.getConversionFor(LogicalTypes.fromSchema(timestampMillisSchema));
        timestampConversion.fromLong(value, timestampMillisSchema, customTimestampMillis);

        Assert.assertEquals(timestampConversion.toLong(value, timestampMillisSchema, customTimestampMillis), value);
    }

    @Test
    public void testTimestampMillisLogicalTypeWithZero() {

        CustomLogicalTypes.CustomTimestampMillis customTimestampMillis = CustomLogicalTypes.timestampMillisConversion();
        Schema schema = SchemaBuilder.builder().longType();
        customTimestampMillis.validate(schema);
        Schema timestampMillisSchema = customTimestampMillis.addToSchema(schema);

        Long value = 0L;
        Conversion timestampConversion = genericData.getConversionFor(LogicalTypes.fromSchema(timestampMillisSchema));
        timestampConversion.fromLong(value, timestampMillisSchema, customTimestampMillis);

        Assert.assertEquals(timestampConversion.toLong(value, timestampMillisSchema, customTimestampMillis), value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimestampMillisLogicalTypeWithNegative() {

        CustomLogicalTypes.CustomTimestampMillis customTimestampMillis = CustomLogicalTypes.timestampMillisConversion();
        Schema schema = SchemaBuilder.builder().longType();
        customTimestampMillis.validate(schema);
        Schema timestampMillisSchema = customTimestampMillis.addToSchema(schema);

        Long value = -1530L;
        Conversion timestampConversion = genericData.getConversionFor(LogicalTypes.fromSchema(timestampMillisSchema));
        timestampConversion.fromLong(value, timestampMillisSchema, customTimestampMillis);
    }

}