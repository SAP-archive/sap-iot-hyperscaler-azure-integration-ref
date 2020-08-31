package com.sap.iot.azure.ref.notification.util;

import com.sap.iot.azure.ref.integration.commons.adx.ADXConstants;
import com.sap.iot.azure.ref.integration.commons.exception.AvroIngestionException;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.ErrorType;
import com.sap.iot.azure.ref.notification.exception.NotificationErrorType;
import com.sap.iot.azure.ref.notification.exception.NotificationProcessException;

public class ModelAbstractionToADXDataTypeMapper {
    /**
     * Maps a given model abstraction data type to the according ADX data type.
     *
     * @param dataType, model abstraction data type
     * @return mapped ADX data type
     */
    public static String getADXDataType(String dataType) {
        switch (dataType) {
            case "String":
            case "LargeString":
                return ADXConstants.ADX_DATATYPE_STRING;
            case "Numeric":
                return "int";
            case "NumericFlexible":
                return "real";
            case "Timestamp":
            case "DateTime":
            case "Date":
                return "datetime";
            case "Boolean":
                return "bool";
            case "JSON":
                return "dynamic";
            default:
                throw new NotificationProcessException(String.format("Unsupported Datatype: %s.", dataType), NotificationErrorType.DATA_TYPE_ERROR,
                        IdentifierUtil.empty(), false);
        }
    }
}
