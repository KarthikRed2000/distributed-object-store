package com.distributed.store.storage.engine;

import com.distributed.store.common.model.Chunk;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class DiskStorage {
    private final Path rootDirectory;

    public DiskStorage(String storagePath) throws IOException {
        this.rootDirectory = Paths.get(storagePath);

        if(!Files.exists(this.rootDirectory)){
            Files.createDirectories(this.rootDirectory);
        }
    }

    public void save(Chunk chunk) throws IOException {
        Path filePath = rootDirectory.resolve(chunk.getId());

        Path tempPath = rootDirectory.resolve(chunk.getId() + ".tmp");

        try (FileOutputStream fos = new FileOutputStream(tempPath.toFile())){
            fos.write(chunk.getData());
            fos.getFD().sync();
        }

        Files.move(tempPath, filePath, StandardCopyOption.ATOMIC_MOVE);
        System.out.println("Saved chunk: " + chunk.getId());
    }

    public Chunk load(String chunkId) throws IOException {
        Path filePath = rootDirectory.resolve(chunkId);
        if (!Files.exists(filePath)){
            throw new IOException("Chunk not found: " + chunkId);
        }
        byte[] data = Files.readAllBytes(filePath);
        return new Chunk(chunkId, data);
    }

    public boolean exists(String chunkId) {
        return Files.exists(rootDirectory.resolve(chunkId));
    }
}