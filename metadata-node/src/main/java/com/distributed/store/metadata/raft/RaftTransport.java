package com.distributed.store.metadata.raft;

import com.distributed.store.common.model.RaftMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RaftTransport {
    private final Map<String, String> clusterTopology;
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(4);
    private final Map<String, Channel> connectionCache = new ConcurrentHashMap<>();

    private MessageHandler messageHandler;

    public RaftTransport(Map<String, String> clusterTopology) {
        this.clusterTopology = clusterTopology;
    }

    public void registerHandler(MessageHandler handler) {
        this.messageHandler = handler;
    }

    public void send(String nodeId, RaftMessage message) {
        String address = clusterTopology.get(nodeId);
        if (nodeId==null){
            System.err.println("CRITICAL: Attempted to send to unknown node: " + nodeId);
            return;
        }
        sendToPeer(address, message);
    }

    public int getPeerCount(){
        return clusterTopology.size() - 1;
    }

    public void broadcast(RaftMessage message) {
        for (Map.Entry<String, String> entry : clusterTopology.entrySet()) {
            String nodeId = entry.getKey();
            String address = entry.getValue();


            if (!nodeId.equals(message.getSenderId())) {
                sendToPeer(address, message);
            }
        }
    }

    private void sendToPeer(String address, RaftMessage message) {
        // Check if we already have an open connection
        Channel existingChannel = connectionCache.get(address);

        if (existingChannel != null && existingChannel.isActive()) {
            // FAST PATH: Reuse connection
            existingChannel.writeAndFlush(message);
        } else {
            // SLOW PATH: Connect for the first time (or reconnect)
            connectAndSend(address, message);
        }
    }

    private void connectAndSend(String address, RaftMessage message) {
        String[] parts = address.split(":");
        if (parts.length != 2) {
            System.err.println("Invalid address format: " + address);
            return;
        }
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        Bootstrap b = new Bootstrap();
        b.group(workerGroup) // Reuse the shared thread pool
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                                new ObjectEncoder(),
                                new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                new SimpleChannelInboundHandler<RaftMessage>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) {
                                        if (messageHandler != null) {
                                            messageHandler.onMessage(msg);
                                        }
                                    }
                                }
                        );
                    }
                });

        // Async connection
        b.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                Channel channel = future.channel();
                // Store in cache for next time!
                connectionCache.put(address, channel);
                channel.writeAndFlush(message);
            } else {
                // Peer is likely down, which is fine in Raft
                System.out.println("Peer down: " + address);
            }
        });
    }

    // Call this when shutting down the node
    public void stop() {
        workerGroup.shutdownGracefully();
    }
}