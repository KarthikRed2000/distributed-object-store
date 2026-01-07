package com.distributed.store.metadata.raft;

import com.distributed.store.common.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SimpleDiskLog {
    private static final Logger logger = LoggerFactory.getLogger(SimpleDiskLog.class);
    private final File file;

    public SimpleDiskLog(String nodeId) {
        this.file = new File(nodeId + ".log");
    }

    public synchronized void save(List<LogEntry> log) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (LogEntry entry : log) {
                // Format: Term:Command
                writer.write(entry.getTerm() + ":" + entry.getCommand());
                writer.newLine();
            }
            // Force write to disk immediately (fsync)
            writer.flush();
        } catch (IOException e) {
            logger.error("Failed to save log to disk", e);
        }
    }

    public synchronized List<LogEntry> load() {
        List<LogEntry> log = new ArrayList<>();
        if (!file.exists()) {
            return log;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int splitIndex = line.indexOf(':');
                if (splitIndex != -1) {
                    int term = Integer.parseInt(line.substring(0, splitIndex));
                    String command = line.substring(splitIndex + 1);
                    log.add(new LogEntry(term, command));
                }
            }
            logger.info("Loaded {} entries from disk.", log.size());
        } catch (IOException e) {
            logger.error("Failed to load log from disk", e);
        }
        return log;
    }
}