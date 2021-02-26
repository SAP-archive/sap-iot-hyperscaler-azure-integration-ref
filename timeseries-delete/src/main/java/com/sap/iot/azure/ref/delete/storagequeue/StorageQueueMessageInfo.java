package com.sap.iot.azure.ref.delete.storagequeue;

import com.microsoft.azure.storage.queue.CloudQueueMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Optional;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StorageQueueMessageInfo {
    CloudQueueMessage cloudQueueMessage;
    Optional<Date> nextVisibleTimeOpt;
}
