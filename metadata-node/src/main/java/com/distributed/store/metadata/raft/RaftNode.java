package com.distributed.store.metadata.raft;

import com.distributed.store.common.model.RaftMessage;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode {

    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private final String nodeId;
    private volatile State state = State.FOLLOWER;
    private volatile int currentTerm = 0;
    private volatile String votedFor = null;

    // Thread-safe vote counting
    private final AtomicInteger votesReceived = new AtomicInteger(0);
    private final Set<String> votedNodes = Collections.synchronizedSet(new HashSet<>());

    private volatile long lastHeartbeatTime;
    private volatile long electionTimeout;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Random random = new Random();
    private final RaftTransport transport;

    public RaftNode(String nodeId, RaftTransport transport) {
        this.nodeId = nodeId;
        this.transport = transport;

        this.transport.registerHandler(this::handleMessage);

        resetElectionTimer();
        startElectionTimer();
    }

    // --- TIMING LOGIC ---

    private void resetElectionTimer() {
        this.lastHeartbeatTime = System.currentTimeMillis();
        // Random timeout 300ms - 600ms
        this.electionTimeout = 300 + random.nextInt(300);
    }

    private void startElectionTimer() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (state == State.LEADER) {
                    sendHeartbeats();
                    return;
                }

                if (System.currentTimeMillis() - lastHeartbeatTime > electionTimeout) {
                    System.out.println(nodeId + ": Election Timeout! (" + electionTimeout + "ms). Starting election...");
                    startElection();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
    }

    // --- ELECTION LOGIC ---

    private void startElection() {
        state = State.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;

        // Reset counts
        votesReceived.set(1); // Vote for self
        votedNodes.clear();
        votedNodes.add(nodeId);

        resetElectionTimer();

        System.out.println(nodeId + ": Starting Election for Term " + currentTerm);

        RaftMessage voteRequest = new RaftMessage(RaftMessage.Type.REQUEST_VOTE, currentTerm, nodeId);
        transport.broadcast(voteRequest);
    }

    // --- MESSAGE HANDLING ---

    public RaftMessage handleMessage(RaftMessage msg) {
        // DEBUG LOG: See exactly what hits this node
        if (msg.getType() == RaftMessage.Type.VOTE_RESPONSE) {
            System.out.println(nodeId + ": <<< RECEIVED VOTE RESPONSE from " + msg.getSenderId() + " for Term " + msg.getTerm());
        }

        // Step Down Logic
        if (msg.getTerm() > currentTerm) {
            System.out.println(nodeId + ": Saw higher term " + msg.getTerm() + " from " + msg.getSenderId() + ". Stepping down.");
            currentTerm = msg.getTerm();
            state = State.FOLLOWER;
            votedFor = null;
            resetElectionTimer();
        }

        switch (msg.getType()) {
            case REQUEST_VOTE:
                return handleRequestVote(msg);
            case VOTE_RESPONSE:
                handleVoteResponse(msg);
                return null; // We consumed the response, nothing to return
            case APPEND_ENTRIES:
                return handleHeartbeat(msg);
            default:
                return null;
        }
    }

    private RaftMessage handleRequestVote(RaftMessage msg) {
        boolean voteGranted = false;

        if (msg.getTerm() >= currentTerm && (votedFor == null || votedFor.equals(msg.getSenderId()))) {
            votedFor = msg.getSenderId();
            voteGranted = true;
            resetElectionTimer();
            System.out.println(nodeId + ": Voted for " + msg.getSenderId() + " in term " + currentTerm);
        }

        RaftMessage response = new RaftMessage(RaftMessage.Type.VOTE_RESPONSE, currentTerm, nodeId);
        response.setSuccess(voteGranted);

        // --- CRITICAL CHECK ---
        // Ideally we want to do: transport.send(msg.getSenderId(), response);
        // But since we rely on returning the message, ensure the transport sends it!
        if (transport != null) {
            transport.send(msg.getSenderId(), response);
        }
        return null;
    }

    private void handleVoteResponse(RaftMessage msg) {
        // Detailed Logic Check with Logging
        if (state != State.CANDIDATE) {
            // System.out.println(nodeId + ": Ignored vote (Not Candidate)");
            return;
        }
        if (msg.getTerm() != currentTerm) {
            System.out.println(nodeId + ": Ignored vote (Term mismatch. My: " + currentTerm + ", Msg: " + msg.getTerm() + ")");
            return;
        }
        if (!msg.isSuccess()) {
            System.out.println(nodeId + ": Vote denied by " + msg.getSenderId());
            return;
        }

        if (!votedNodes.contains(msg.getSenderId())) {
            votedNodes.add(msg.getSenderId());
            int totalVotes = votesReceived.incrementAndGet();
            System.out.println(nodeId + ": Counted vote from " + msg.getSenderId() + " (Total: " + totalVotes + ")");

            // Majority Check ( > 1.5, so 2 or more)
            if (totalVotes >= 2) {
                becomeLeader();
            }
        }
    }

    private RaftMessage handleHeartbeat(RaftMessage msg) {
        state = State.FOLLOWER;
        resetElectionTimer();

        // System.out.println(nodeId + ": Heartbeat from " + msg.getSenderId());

        RaftMessage response = new RaftMessage(RaftMessage.Type.APPEND_RESPONSE, currentTerm, nodeId);
        response.setSuccess(true);
        return response;
    }

    private void becomeLeader() {
        if (state != State.LEADER) {
            state = State.LEADER;
            System.out.println("\n" + nodeId + ": --- I AM LEADER (Term " + currentTerm + ") ---\n");
            sendHeartbeats();
        }
    }

    private void sendHeartbeats() {
        RaftMessage heartbeat = new RaftMessage(RaftMessage.Type.APPEND_ENTRIES, currentTerm, nodeId);
        transport.broadcast(heartbeat);
    }
}