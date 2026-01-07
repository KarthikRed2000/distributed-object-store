package com.distributed.store;

import com.distributed.store.config.ClusterConfig;
import com.distributed.store.metadata.MetadataServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A utility to run a Raft Node from the command line without creating a config file.
 * It bridges CLI arguments to the ClusterConfig object required by MetadataServer.
 */
public class NodeRunner {
    private static final Logger logger = LoggerFactory.getLogger(NodeRunner.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: NodeRunner <NodeId> <Port> <Peers>");
            System.out.println("Example: Node_A 9001 Node_B:localhost:9002,Node_C:localhost:9003");
            return;
        }

        String nodeId = args[0];
        int localPort = Integer.parseInt(args[1]);
        String peersRaw = args[2];

        // 1. Parse the peers argument into a Map
        Map<String, String> peersMap = new HashMap<>();
        if (!peersRaw.equalsIgnoreCase("none")) {
            for (String part : peersRaw.split(",")) {
                String[] info = part.split(":");
                // Expecting format: ID:HOST:PORT
                if (info.length == 3) {
                    peersMap.put(info[0], info[1] + ":" + info[2]);
                } else {
                    logger.warn("Skipping invalid peer format: {}", part);
                }
            }
        }

        try {
            // 2. Create a "Synthetic" ClusterConfig
            // We override the methods to return our CLI data instead of reading a file.
            ClusterConfig manualConfig = new ClusterConfig() {
                @Override
                public int getPort(String id) {
                    if (id.equals(nodeId)) {
                        return localPort;
                    }
                    // Try to find port in peers map for other nodes (if needed)
                    if (peersMap.containsKey(id)) {
                        String[] parts = peersMap.get(id).split(":");
                        return Integer.parseInt(parts[1]);
                    }
                    return 0; // Should not happen for local node
                }

                @Override
                public Map<String, String> getPeers() {
                    return peersMap;
                }
            };

            // 3. Start the Server with the manual config
            MetadataServer server = new MetadataServer(nodeId, manualConfig);
            server.start();

        } catch (Exception e) {
            logger.error("Failed to start NodeRunner", e);
        }
    }
}