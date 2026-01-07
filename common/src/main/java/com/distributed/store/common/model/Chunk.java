package com.distributed.store.common.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.zip.CRC32;

public class Chunk implements Serializable {
    private final String id;
    private final byte[] data;
    private final long checksum;

    public Chunk(String id, byte[] data) {
        this.id = id;
        this.data = data;
        this.checksum = calculateChecksum(data);
    }

    private long calculateChecksum(byte[] input) {
        CRC32 crc = new CRC32();
        crc.update(input);
        return crc.getValue();
    }

    public boolean isValid() {
        return calculateChecksum(this.data) == this.checksum;
    }

    public String getId() { return id; }
    public byte[] getData() { return data; }
}