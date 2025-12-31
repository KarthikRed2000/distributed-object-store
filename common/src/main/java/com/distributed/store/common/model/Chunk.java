package com.distributed.store.common.model;

import java.io.Serializable;
/**
 * Represents a standard unit of data in our system.
 * This could be a shard of an Erasure Coded file, or a full Replica.
 */
public class Chunk implements Serializable {
    private final String id;
    private final byte[] data;
    private final long checksum;

    public Chunk(String id, byte[] data) {
        this.id = id;
        this.data = data;
        this.checksum = calculateChecksum(data);
    }

    // Simple checksum (CRC32 is better)
    long calculateChecksum(byte[] data) {
        long sum = 0;
        for (byte b : data) {
            sum += b;
        }
        return sum;
    }

    public String getId() {
        return id;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isValid() {
        return checksum == calculateChecksum(data);
    }

    @Override
    public String toString() {
        return "Chunk{id='"+ id + "', size=" + data.length + " bytes";
    }

}