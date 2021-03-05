package com.sap.iot.azure.ref.integration.commons.model;

/**
 * All POJOs that are deserialized from an event in external queue (Azure Event Hub / Storge Queue) shall implement this interface.
 * This interface provides an accessor to the source of the message (e.g., partition id, offset, etc.,  in case of Azure Event Hub)
 *
 * @param <S> message metadata based on the queue type e.g., {@link com.sap.iot.azure.ref.integration.commons.model.base.eventhub.SystemProperties}
 */
public interface MessageEntity<S> {

    /**
     * returns the message metadata from the source queue
     * @return message metadata
     */
    S getSource();

    /**
     * sets the message metadata
     * @param s message metadata
     */
    void setSource(S s);


    /**
     * adds a message source details to the current message
     * @param s message metadata
     * @return message with metadata
     */
    default MessageEntity<S> withSource(S s) {
        setSource(s);
        return this;
    }
}