package com.distributed.store.metadata.raft;

import com.distributed.store.common.model.LogEntry;
import com.distributed.store.common.model.RaftMessage;
import com.distributed.store.common.model.RaftMessage.Type;
import com.distributed.store.metadata.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

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
    private List<LogEntry> log = new ArrayList<>();
    private int commitIndex = 0;
    private final SimpleDiskLog diskLog;

    // Leadership State (Only used when Leader)
    private final Map<String, Integer> matchIndex = new HashMap<>();

    // --- DYNAMIC CLUSTER CONFIGURATION ---
    private final List<String> activeStorageNodes = new CopyOnWriteArrayList<>();
    private int nextStorageNodeIndex = 0;

    // Timers
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> electionTimeoutTask;
    private ScheduledFuture<?> heartbeatTask;
    private final Random random = new Random();

    // Tuning
    private static final int MIN_TIMEOUT = 2000;
    private static final int MAX_TIMEOUT = 4000;
    private static final int HEARTBEAT_INTERVAL = 500;

    private final StateMachine stateMachine = new StateMachine();
    private int lastApplied = 0;

    private final Map<String, Long> workerHeartbeats = new ConcurrentHashMap<>();
    private static final long WORKER_TIMEOUT_MS = 10_000;

    public RaftNode(String nodeId, RaftTransport transport) {
        this.nodeId = nodeId;
        this.transport = transport;
        this.diskLog = new SimpleDiskLog(nodeId);
        this.log = diskLog.load();
        resetElectionTimer();
    }

    // --- CLIENT API ---

    public synchronized boolean replicate(String command) {
        if (currentRole != Role.LEADER) {
            logger.warn("Received command '{}', but I am not Leader (Leader is: {})", command, leaderId);
            return false;
        }

        // 1. Append to local log
        LogEntry entry = new LogEntry(currentTerm, command);
        log.add(entry);
        persist();
        logger.info("Leader appended entry at index {}: {}", log.size() - 1, entry);

        // 2. Broadcast to followers immediately
        sendAppendEntries();
        return true;
    }

    // --- MESSAGE HANDLING ---

    public synchronized RaftMessage handleMessage(RaftMessage msg) {
        if (msg.getTerm() > currentTerm) {
            currentTerm = msg.getTerm();
            becomeFollower(msg.getTerm());
        }

        switch (msg.getType()) {
            case CLIENT_COMMAND:    return handleClientCommand(msg);
            case REQUEST_VOTE:      return handleRequestVote(msg);
            case APPEND_ENTRIES:    return handleAppendEntries(msg);
            case VOTE_RESPONSE:     handleVoteResponse(msg); return null;
            case REGISTER_WORKER:   return handleRegisterWorker(msg);
            case HEARTBEAT_RESPONSE:
                if (currentRole == Role.LEADER) {
                    handleHeartbeatResponse(msg);
                }
                return null;
            default: return null;
        }
    }

    // --- NEW: SERVICE DISCOVERY HANDLER ---
    private RaftMessage handleRegisterWorker(RaftMessage msg) {
        if (currentRole == Role.LEADER) {
            String newWorker = (String) msg.getPayload();
            long now = System.currentTimeMillis();

            // Update timestamp
            workerHeartbeats.put(newWorker, now);

            if (newWorker != null && !activeStorageNodes.contains(newWorker)) {
                activeStorageNodes.add(newWorker);
                logger.info("NEW WORKER REGISTERED: " + newWorker);
            }
            return new RaftMessage(Type.CLIENT_RESPONSE, nodeId, currentTerm, "ACK");
        } else {
            // If we aren't leader, we could redirect, but for simplicity we ignore or return null
            return null;
        }
    }

    private void startWorkerHealthCheck() {
        scheduler.scheduleAtFixedRate(this::checkDeadWorkers, 5, 5, TimeUnit.SECONDS);
    }

    private void checkDeadWorkers() {
        if (currentRole != Role.LEADER) return;

        long now = System.currentTimeMillis();
        for (String worker : activeStorageNodes) {
            Long lastSeen = workerHeartbeats.get(worker);

            if (lastSeen == null || (now - lastSeen) > WORKER_TIMEOUT_MS) {
                logger.warn("DETECTED DEAD WORKER: " + worker + " (Last seen " + (now - lastSeen) + "ms ago)");

                // Remove from the pool immediately
                activeStorageNodes.remove(worker);
                workerHeartbeats.remove(worker);
            }
        }
    }

    private RaftMessage handleClientCommand(RaftMessage msg) {
        String cmd = (String) msg.getPayload();

        // 1. Handle ALLOCATION (Load Balancing)
        if (cmd.equals("ALLOCATE")) {
            return handleAllocationRequest();
        }
        // 2. Handle Reads (GET)
        else if (cmd.startsWith("GET ")) {
            return handleClientGETQuery(cmd);
        }
        // 3. Handle Writes (SET)
        else {
            return handleClientSETCommand(cmd);
        }
    }

    // --- NEW: ALLOCATION LOGIC ---
    private RaftMessage handleAllocationRequest() {
        if (currentRole != Role.LEADER) {
            return new RaftMessage(Type.CLIENT_RESPONSE, nodeId, currentTerm, "REDIRECT:" + leaderId);
        }

        if (activeStorageNodes.isEmpty()) {
            RaftMessage response = new RaftMessage(Type.CLIENT_RESPONSE, nodeId, currentTerm, false);
            response.setPayload("ERROR: No Storage Nodes Available");
            return response;
        }

        String primary = activeStorageNodes.get(nextStorageNodeIndex);
        nextStorageNodeIndex = (nextStorageNodeIndex + 1) % activeStorageNodes.size();

        String replica = null;
        if(activeStorageNodes.size() > 1){
            replica = activeStorageNodes.get(nextStorageNodeIndex);
            nextStorageNodeIndex = (nextStorageNodeIndex + 1) % activeStorageNodes.size();
        }

        String allocation = primary;
        if (replica != null) {
            allocation += "," + replica;
        }

        logger.info("Allocating Chain: " + allocation);
        RaftMessage response = new RaftMessage(Type.CLIENT_RESPONSE, nodeId, currentTerm, true);
        response.setPayload(allocation);
        return response;
    }

    private RaftMessage handleClientGETQuery(String cmd) {
        String key = cmd.substring(4).trim();
        String value = stateMachine.get(key);

        RaftMessage response = new RaftMessage(Type.CLIENT_COMMAND, nodeId, currentTerm, true);
        response.setPayload(value != null ? value : "(null)");
        return response;
    }

    private RaftMessage handleClientSETCommand(String cmd) {
        // SET requires Consensus via Log Replication
        boolean success = replicate(cmd);
        RaftMessage response = new RaftMessage(Type.CLIENT_COMMAND, nodeId, currentTerm, success);
        response.setPayload(success ? "OK" : "ERROR");
        return response;
    }

    private RaftMessage handleRequestVote(RaftMessage msg) {
        boolean granted = false;
        if (msg.getTerm() >= currentTerm && (votedFor == null || votedFor.equals(msg.getSenderId()))) {
            votedFor = msg.getSenderId();
            granted = true;
            resetElectionTimer();
            logger.info("Voted FOR {} in term {}", msg.getSenderId(), currentTerm);
        }
        return new RaftMessage(Type.VOTE_RESPONSE, nodeId, currentTerm, granted);
    }

    private RaftMessage handleAppendEntries(RaftMessage msg) {
        // 1. Standard Term Check
        if (msg.getTerm() < currentTerm) {
            return new RaftMessage(Type.HEARTBEAT_RESPONSE, nodeId, currentTerm, false);
        }

        // 2. Acknowledge Leader
        this.leaderId = msg.getSenderId();
        becomeFollower(msg.getTerm());
        resetElectionTimer();

        List<LogEntry> leaderEntries = msg.getEntries();

        // 3. LOG REPAIR & CONFLICT RESOLUTION
        int i = 0;
        while (i < leaderEntries.size()) {
            LogEntry leaderEntry = leaderEntries.get(i);

            if (i < log.size()) {
                LogEntry localEntry = log.get(i);
                // Conflict detected (Same index, different term)
                if (localEntry.getTerm() != leaderEntry.getTerm()) {
                    logger.warn("Conflict at index {}. Truncating log from {} to {}", i, localEntry, leaderEntry);
                    // Delete this and everything after
                    while (log.size() > i) {
                        log.remove(log.size() - 1);
                    }
                    log.add(leaderEntry);
                    persist();
                }
            } else {
                // New entry
                log.add(leaderEntry);
                persist();
            }
            i++;
        }

        // 4. Update Commit Index
        if (msg.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(msg.getLeaderCommit(), log.size());
            applyCommits();
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

    private void handleHeartbeatResponse(RaftMessage msg) {
        if (msg.isSuccess()) {
            matchIndex.put(msg.getSenderId(), log.size());
            updateCommitIndex();
        }
    }

    private void updateCommitIndex() {
        for (int n = log.size(); n > commitIndex; n--) {
            int replicationCount = 1; // Count Leader (me)

            for (Integer followerIndex : matchIndex.values()) {
                if (followerIndex >= n) {
                    replicationCount++;
                }
            }
            if (replicationCount > (transport.getPeerCount() + 1) / 2) {
                commitIndex = n;
                applyCommits();
                break;
            }
        }
    }

    private void becomeFollower(int term) {
        currentTerm = term;
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
        transport.broadcast(new RaftMessage(Type.REQUEST_VOTE, nodeId, currentTerm));
        resetElectionTimer();
    }

    private void becomeLeader() {
        if (currentRole == Role.LEADER) return;
        currentRole = Role.LEADER;
        logger.info("!!! BECAME LEADER !!! Term {}", currentTerm);
        matchIndex.clear();
        if (electionTimeoutTask != null) electionTimeoutTask.cancel(false);
        startHeartbeats();
        startWorkerHealthCheck();
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

    // --- STATE MACHINE ---

    private void applyCommits() {
        while (lastApplied < commitIndex) {
            LogEntry entry = log.get(lastApplied);
            stateMachine.apply(entry.getCommand());
            lastApplied++;
        }
    }

    private void persist() {
        diskLog.save(log);
    }
}