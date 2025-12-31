package com.distributed.store.common.model;

import java.io.Serializable;

public class RaftMessage implements Serializable {
    public enum Type {
        REQUEST_VOTE,          // "Vote for me!"
        VOTE_RESPONSE,         // "Yes, I vote for you" or "No"
        APPEND_ENTRIES,        // "I am Leader, here is data" (Heartbeat)
        APPEND_RESPONSE        // "Got it"
    }

    private Type type;
    private int term;
    private String senderId;
    private boolean success;

    public RaftMessage(Type type, int term, String senderId) {
        this.type = type;
        this.term = term;
        this.senderId = senderId;
    }

    public void setSuccess(boolean success) { this.success = success; }

    public boolean isSuccess() { return success; }
    public Type getType() { return type; }
    public int getTerm() { return term; }
    public String getSenderId() { return senderId; }
}
