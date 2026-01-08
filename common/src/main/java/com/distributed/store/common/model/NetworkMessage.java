package com.distributed.store.common.model;

import java.io.Serializable;

public class NetworkMessage implements Serializable {
    public enum Type { PUT, GET, RESPONSE_OK, RESPONSE_ERROR }

    private final Type type;
    private final String key;     // Chunk ID
    private final byte[] data;    // Payload (for PUT or RESPONSE)
    private final String message; // Error message or Status text
    private String replicaTarget;

    // Constructor for Requests (PUT/GET)
    public NetworkMessage(Type type, String key, byte[] data) {
        this.type = type;
        this.key = key;
        this.data = data;
        this.message = null;
    }

    // Constructor for Responses
    public NetworkMessage(Type type, byte[] data, String message) {
        this.type = type;
        this.key = null;
        this.data = data;
        this.message = message;
    }

    public void setReplicaTarget(String target) { this.replicaTarget = target; }
    public String getReplicaTarget() { return replicaTarget; }

    public Type getType() { return type; }
    public String getKey() { return key; }
    public byte[] getData() { return data; }
    public String getMessage() { return message; }
}