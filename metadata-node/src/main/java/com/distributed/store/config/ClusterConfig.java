package com.distributed.store.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ClusterConfig {
    private final Map<String, String> peerMap = new HashMap<>();

    protected ClusterConfig() {
    }

    public ClusterConfig(String configFilePath) throws IOException {
        Properties props = new Properties();

        try (InputStream input = new FileInputStream(configFilePath)) {
            props.load(input);
        }

        // Parse properties starting with "node."
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("node.")) {
                String nodeId = key.substring(5); // Strips "node."
                String address = props.getProperty(key);

                peerMap.put(nodeId, address);
            }
        }

        if (peerMap.isEmpty()) {
            throw new IllegalArgumentException("Configuration file [" + configFilePath + "] contained no 'node.*' entries.");
        }
    }

    /**
     * Returns the full map of peers for the Transport layer.
     */
    public Map<String, String> getPeers() {
        return peerMap;
    }

    /**
     * Helper to get the port for the current node (used by Netty Server).
     */
    public int getPort(String nodeId) {
        String addr = peerMap.get(nodeId);
        if (addr == null) {
            throw new IllegalArgumentException("Node ID [" + nodeId + "] not found in configuration.");
        }
        // Expecting format "host:port"
        String[] parts = addr.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format for " + nodeId + ": " + addr);
        }
        return Integer.parseInt(parts[1]);
    }
}