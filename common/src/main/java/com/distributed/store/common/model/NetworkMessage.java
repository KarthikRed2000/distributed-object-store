package com.distributed.store.common.model;

import java.io.Serializable;

public class NetworkMessage implements Serializable {
    public enum Type {
        PUT,
        GET,
        RESPONSE_OK,
        RESPONSE_ERROR
    }

    private Type type;
    private String key;
    private byte[] data;
    private String message;

    public NetworkMessage(Type type, String key, byte[] data) {
        this.type = type;
        this.key = key;
        this.data = data;
    }

    public NetworkMessage(Type type, byte[] data, String message) {
        this.type = type;
        this.message = message;
        this.data = data;
    }

    public Type getType() {return type;}
    public String getKey() {return key;}
    public byte[] getData() {return data;}
    public String getMessage() {return message;}
}
