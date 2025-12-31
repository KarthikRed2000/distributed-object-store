package com.distributed.store.storage;

import com.distributed.store.common.model.Chunk;
import com.distributed.store.storage.engine.DiskStorage;
import java.nio.charset.StandardCharsets;

public class StorageMain {
    public static void main(String[] args) {
        try {
            DiskStorage engine = new DiskStorage("data_node_1");

            String fileContent = "Hello, Distributed World! This is a test chunk.";
            Chunk myChunk = new Chunk("chunk_001", fileContent.getBytes(StandardCharsets.UTF_8));

            System.out.println("Writing to disk...");
            engine.save(myChunk);

            System.out.println("Reading back...");
            Chunk loadedChunk = engine.load("chunk_001");

            String loadedContent = new String(loadedChunk.getData(), StandardCharsets.UTF_8);
            System.out.println("Content: " + loadedContent);

            if (loadedChunk.isValid()) {
                System.out.println("SUCCESS: Integrity Check Passed!");
            } else {
                System.err.println("FAILURE: Data corruption detected.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}