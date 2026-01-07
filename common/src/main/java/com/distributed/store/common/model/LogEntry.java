package com.distributed.store.common.model;

import java.io.Serializable;

public class LogEntry implements Serializable {
    private final int term;
    private final String command; // e.g., "SET key=value"

    public LogEntry(int term, String command) {
        this.term = term;
        this.command = command;
    }

    public int getTerm() { return term; }
    public String getCommand() { return command; }

    @Override
    public String toString() {
        return "{T:" + term + ", Cmd:'" + command + "'}";
    }
}