package com.distributed.store.metadata.raft;

import com.distributed.store.common.model.RaftMessage;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RaftNode {

    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private final String nodeId;
    private State state = State.FOLLOWER;
    private int currentTerm = 0;
    private String votedFor = null;

    private long lastHeartbeatTime;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Random random = new Random();
    private final RaftTransport transport;

    public RaftNode(String nodeId, RaftTransport transport) {
        this.nodeId = nodeId;
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.transport = transport;
        startElectionTimer();
    }

    private void startElectionTimer() {
        scheduler.scheduleAtFixedRate(()->{
            try {
                if (state == State.LEADER){
                    sendHeartbeats();
                    return;
                }
                long timeout = 150 + random.nextInt(150);

                if (System.currentTimeMillis() - lastHeartbeatTime > timeout) {
                    System.out.println(nodeId + ": Leader is dead! Starting election...");
                    startElection();
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        state = state.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        System.out.println(nodeId + ": Became CANDIDATE for Term " + currentTerm);

        RaftMessage voteRequest = new RaftMessage(RaftMessage.Type.REQUEST_VOTE, currentTerm, nodeId);
        transport.broadcast(voteRequest);
    }

    public RaftMessage handleMessage(RaftMessage msg) {
        // If we see a higher term, step down immediately
        if (msg.getTerm() > currentTerm) {
            currentTerm = msg.getTerm();
            state = State.FOLLOWER;
            votedFor = null;
        }

        switch (msg.getType()) {
            case REQUEST_VOTE:
                return handleRequestVote(msg);
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
            lastHeartbeatTime = System.currentTimeMillis(); // Reset timer, we heard from a valid candidate
        }

        RaftMessage response = new RaftMessage(RaftMessage.Type.VOTE_RESPONSE, currentTerm, nodeId);
        response.setSuccess(voteGranted);
        return response;
    }

    private RaftMessage handleHeartbeat(RaftMessage msg) {
        state = State.FOLLOWER; // Accept the leader
        lastHeartbeatTime = System.currentTimeMillis(); // Reset timer
        System.out.println(nodeId + ": Received heartbeat from Leader " + msg.getSenderId());

        RaftMessage response = new RaftMessage(RaftMessage.Type.APPEND_RESPONSE, currentTerm, nodeId);
        response.setSuccess(true);
        return response;
    }

    private void sendHeartbeats() {
        System.out.println(nodeId + " (LEADER): Sending Heartbeats...");
        RaftMessage heartbeat = new RaftMessage(RaftMessage.Type.APPEND_ENTRIES, currentTerm, nodeId);
        transport.broadcast(heartbeat);
    }


}