package com.distributed.store.common.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RaftMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Type {
        REQUEST_VOTE,
        VOTE_RESPONSE,
        APPEND_ENTRIES,     // Heartbeat OR Data Replication
        HEARTBEAT_RESPONSE, // Ack for AppendEntries
        CLIENT_COMMAND,
        REGISTER_WORKER,    // <--- Service Discovery Type
        CLIENT_RESPONSE
    }

    private final Type type;
    private String senderId;
    private int term;

    // Response field
    private boolean success;

    // Replication fields
    private int prevLogIndex;
    private int prevLogTerm;
    private int leaderCommit;
    private List<LogEntry> entries = new ArrayList<>();

    // Payload for Commands, Responses, and Registration
    private String payload;

    // --- CONSTRUCTORS ---

    // 1. Constructor for Requests (Heartbeat/Replication)
    public RaftMessage(Type type, String senderId, int term, int prevLogIndex, int prevLogTerm, int leaderCommit) {
        this.type = type;
        this.senderId = senderId;
        this.term = term;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
    }

    // 2. Constructor for Simple Notifications (Vote Request)
    public RaftMessage(Type type, String senderId, int term) {
        this(type, senderId, term, 0, 0, 0);
    }

    // 3. Constructor for Responses (Vote Response, Heartbeat Response)
    public RaftMessage(Type type, String senderId, int term, boolean success) {
        this.type = type;
        this.senderId = senderId;
        this.term = term;
        this.success = success;
    }

    // 4. [NEW] Constructor for Payload Messages (Client Command, Register, Redirects)
    // This is the one required by your new StorageServer and RaftNode logic!
    public RaftMessage(Type type, String senderId, int term, String payload) {
        this.type = type;
        this.senderId = senderId;
        this.term = term;
        this.payload = payload;
        this.success = true; // Default to true for commands unless specified otherwise
    }

    // --- SETTERS & GETTERS ---

    public void setEntries(List<LogEntry> entries) {
        this.entries = (entries != null) ? entries : new ArrayList<>();
    }

    public Type getType() { return type; }
    public String getSenderId() { return senderId; }
    public int getTerm() { return term; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public int getPrevLogIndex() { return prevLogIndex; }
    public int getPrevLogTerm() { return prevLogTerm; }
    public int getLeaderCommit() { return leaderCommit; }
    public List<LogEntry> getEntries() { return entries; }

    public void setPayload(String p) { this.payload = p; }
    public String getPayload() { return payload; }

    @Override
    public String toString() {
        if (type == Type.APPEND_ENTRIES) {
            return "Msg{type=" + type + ", term=" + term + ", entries=" + entries.size() + "}";
        }
        if (payload != null) {
            return "Msg{type=" + type + ", term=" + term + ", payload='" + payload + "'}";
        }
        return "Msg{type=" + type + ", term=" + term + ", success=" + success + "}";
    }
}