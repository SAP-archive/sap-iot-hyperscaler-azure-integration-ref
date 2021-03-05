package com.sap.iot.azure.ref.delete.exception;

import com.microsoft.azure.storage.StorageErrorCode;
import com.microsoft.azure.storage.StorageException;

import java.util.Arrays;
import java.util.List;

public class StorageExceptionUtil {

    /**
     * Returns if the {@link StorageException} is transient or non-transient.
     *
     * @param exception {@link StorageException} to be checked
     * @return boolean describing if the exception is transient
     */
    public static boolean isTransient(StorageException exception) {
        final List<String> TRANSIENT_CODES = Arrays.asList(
                StorageErrorCode.BAD_GATEWAY.toString(),
                StorageErrorCode.SERVICE_INTERNAL_ERROR.toString(),
                StorageErrorCode.SERVICE_TIMEOUT.toString(),
                StorageErrorCode.TRANSPORT_ERROR.toString(),
                StorageErrorCode.SERVER_BUSY.toString()
        );

        return TRANSIENT_CODES.contains(exception.getErrorCode());
    }
}
