package com.distributed.store.metadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClusterLauncher {

    public static void main(String[] args) throws IOException, InterruptedException {
        // 1. Define the nodes we want to run
        // Format: {Port, NodeID, Peer1, Peer2}
        String[][] nodeConfigs = {
                {"9001", "Node_A", "localhost:9002", "localhost:9003"},
                {"9002", "Node_B", "localhost:9001", "localhost:9003"},
                {"9003", "Node_C", "localhost:9001", "localhost:9002"}
        };

        List<Process> processes = new ArrayList<>();

        // 2. Launch each node in its own process
        for (String[] config : nodeConfigs) {
            processes.add(startNode(config));
        }

        System.out.println("--- CLUSTER STARTED (Press Enter to Stop) ---");
        System.out.println("Logs will be interleaved below:");

        // 3. Keep running until user presses Enter
        System.in.read();

        // 4. Cleanup: Kill all nodes when we exit
        System.out.println("Shutting down cluster...");
        for (Process p : processes) {
            p.destroy(); // Sends SIGTERM
        }
    }

    private static Process startNode(String[] args) throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = MetadataServer.class.getName();

        // Build the command: java -cp ... MetadataServer <args>
        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-cp");
        command.add(classpath);
        command.add(className);

        // Add the specific args for this node (Port, ID, Peers...)
        for (String arg : args) {
            command.add(arg);
        }

        ProcessBuilder builder = new ProcessBuilder(command);

        // IMPORTANT: Direct the node's logs to our main console
        // This lets you see the "election" happening in real-time.
        builder.inheritIO();

        return builder.start();
    }
}