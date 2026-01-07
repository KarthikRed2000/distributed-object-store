package com.distributed.store.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StateMachine {
    private static final Logger logger = LoggerFactory.getLogger(StateMachine.class);

    // The actual database
    private final Map<String, String> kvStore = new ConcurrentHashMap<>();

    public void apply(String command) {
        // Parse simple commands: "SET key=value"
        try {
            if (command.startsWith("SET ")) {
                String[] parts = command.substring(4).split("=");
                if (parts.length == 2) {
                    kvStore.put(parts[0], parts[1]);
                    logger.info("Applied to StateMachine: {} = {}", parts[0], parts[1]);
                }
            } else if (command.startsWith("DEL ")) {
                String key = command.substring(4);
                kvStore.remove(key);
                logger.info("Removed from StateMachine: {}", key);
            }
        } catch (Exception e) {
            logger.error("Failed to apply command: {}", command, e);
        }
    }

    public String get(String key) {
        return kvStore.get(key);
    }
}