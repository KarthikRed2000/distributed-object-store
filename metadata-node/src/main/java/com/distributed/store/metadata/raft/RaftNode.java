package com.distributed.store.metadata.raft;

import com.distributed.store.common.model.LogEntry;
import com.distributed.store.common.model.RaftMessage;
import com.distributed.store.common.model.RaftMessage.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RaftNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final String nodeId;
    private final RaftTransport transport;

    // State
    private volatile Role currentRole = Role.FOLLOWER;
    private int currentTerm = 0;
    private String votedFor = null;
    private String leaderId = null;

    // Log Storage
    private final List<LogEntry> log = new ArrayList<>();
    private int commitIndex = 0;

    // Timers
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> electionTimeoutTask;
    private ScheduledFuture<?> heartbeatTask;
    private final Random random = new Random();

    // Tuning
    private static final int MIN_TIMEOUT = 2000;
    private static final int MAX_TIMEOUT = 4000;
    private static final int HEARTBEAT_INTERVAL = 500;

    public RaftNode(String nodeId, RaftTransport transport) {
        this.nodeId = nodeId;
        this.transport = transport;
        resetElectionTimer();
    }

    // --- CLIENT API ---

    /**
     * Called when a client sends a command to this node.
     * Returns true if queued, false if not Leader.
     */
    public synchronized boolean replicate(String command) {
        if (currentRole != Role.LEADER) {
            logger.warn("Received command '{}', but I am not Leader (Leader is: {})", command, leaderId);
            return false;
        }

        // 1. Append to local log
        LogEntry entry = new LogEntry(currentTerm, command);
        log.add(entry);
        logger.info("Leader appended entry at index {}: {}", log.size() - 1, entry);

        // 2. Broadcast to followers immediately
        sendAppendEntries();
        return true;
    }

    // --- MESSAGE HANDLING ---

    public synchronized RaftMessage handleMessage(RaftMessage msg) {
        if (msg.getTerm() > currentTerm) {
            currentTerm = msg.getTerm();
            becomeFollower();
        }

        switch (msg.getType()) {
            case CLIENT_COMMAND: return handleClietnCommand(msg);
            case REQUEST_VOTE:      return handleRequestVote(msg);
            case APPEND_ENTRIES:    return handleAppendEntries(msg);
            case VOTE_RESPONSE:     handleVoteResponse(msg); return null;
            case HEARTBEAT_RESPONSE: return null; // TODO: Handle commits
            default: return null;
        }
    }

    private RaftMessage handleClietnCommand(RaftMessage msg) {
        String cmd = msg.getPayload();
        logger.info("Received command {}", cmd);

        boolean success = replicate(cmd);

        RaftMessage newMessage = new RaftMessage(Type.CLIENT_COMMAND, nodeId, currentTerm);
        newMessage.setSuccess(success);
        return newMessage;
    }

    private RaftMessage handleRequestVote(RaftMessage msg) {
        boolean granted = false;
        if (msg.getTerm() >= currentTerm && (votedFor == null || votedFor.equals(msg.getSenderId()))) {
            // Simplified check: In real Raft, we also check if candidate's log is up-to-date
            votedFor = msg.getSenderId();
            granted = true;
            resetElectionTimer();
            logger.info("Voted FOR {} in term {}", msg.getSenderId(), currentTerm);
        }
        return new RaftMessage(Type.VOTE_RESPONSE, nodeId, currentTerm, granted);
    }

    private RaftMessage handleAppendEntries(RaftMessage msg) {
        if (msg.getTerm() < currentTerm) {
            return new RaftMessage(Type.HEARTBEAT_RESPONSE, nodeId, currentTerm, false);
        }

        this.leaderId = msg.getSenderId();
        becomeFollower(); // Refresh follower state
        resetElectionTimer();

        // 1. Append any new entries from Leader
        if (!msg.getEntries().isEmpty()) {
            log.addAll(msg.getEntries());
            logger.info("Follower appended {} entries. Log Size: {}", msg.getEntries().size(), log.size());
        }

        // 2. Update Commit Index
        if (msg.getLeaderCommit() > commitIndex) {
            // commitIndex = min(leaderCommit, index of last new entry)
            commitIndex = Math.min(msg.getLeaderCommit(), log.size() - 1);
            logger.debug("Follower commitIndex updated to {}", commitIndex);
        }

        return new RaftMessage(Type.HEARTBEAT_RESPONSE, nodeId, currentTerm, true);
    }

    // --- LEADERSHIP LOGIC ---

    private int votesReceived = 0;

    private void handleVoteResponse(RaftMessage msg) {
        if (currentRole == Role.CANDIDATE && msg.isSuccess() && msg.getTerm() == currentTerm) {
            votesReceived++;
            int quorum = (transport.getPeerCount() + 1) / 2 + 1;
            if (votesReceived >= quorum) {
                becomeLeader();
            }
        }
    }

    private void becomeFollower() {
        currentRole = Role.FOLLOWER;
        votedFor = null;
        if (heartbeatTask != null) heartbeatTask.cancel(false);
        resetElectionTimer();
    }

    private void becomeCandidate() {
        currentRole = Role.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        votesReceived = 1;

        logger.info("Candidate for Term {}", currentTerm);

        // Broadcast Vote Request
        RaftMessage request = new RaftMessage(Type.REQUEST_VOTE, nodeId, currentTerm);
        transport.broadcast(request);

        resetElectionTimer();
    }

    private void becomeLeader() {
        if (currentRole == Role.LEADER) return;
        currentRole = Role.LEADER;
        logger.info("!!! BECAME LEADER !!! Term {}", currentTerm);

        if (electionTimeoutTask != null) electionTimeoutTask.cancel(false);
        startHeartbeats();
    }

    // --- HEARTBEATS ---

    private void startHeartbeats() {
        heartbeatTask = scheduler.scheduleAtFixedRate(() -> {
            synchronized (this) {
                if (currentRole != Role.LEADER) {
                    if (heartbeatTask != null) heartbeatTask.cancel(false);
                    return;
                }
                sendAppendEntries();
            }
        }, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void sendAppendEntries() {
        // Simplified: We assume Followers are always insync for now
        int prevLogIndex = Math.max(0, log.size() - 1);
        int prevLogTerm = log.isEmpty() ? 0 : log.get(prevLogIndex).getTerm();

        RaftMessage msg = new RaftMessage(
                Type.APPEND_ENTRIES,
                nodeId,
                currentTerm,
                prevLogIndex,
                prevLogTerm,
                commitIndex
        );

        // In a real implementation, we would send ONLY new entries based on nextIndex[]
        // For this step, we are just maintaining the heartbeat mechanism
        msg.setEntries(new ArrayList<>(log));

        transport.broadcast(msg);
    }

    private void resetElectionTimer() {
        if (currentRole == Role.LEADER) return;
        if (electionTimeoutTask != null) electionTimeoutTask.cancel(false);

        long delay = MIN_TIMEOUT + random.nextInt(MAX_TIMEOUT - MIN_TIMEOUT);
        electionTimeoutTask = scheduler.schedule(this::onElectionTimeout, delay, TimeUnit.MILLISECONDS);
    }

    private void onElectionTimeout() {
        synchronized (this) {
            if (currentRole != Role.LEADER) {
                becomeCandidate();
            }
        }
    }
}