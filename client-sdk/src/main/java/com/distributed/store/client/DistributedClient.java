package com.distributed.store.client;

import com.distributed.store.common.model.NetworkMessage;
import com.distributed.store.common.model.RaftMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.*;

public class DistributedClient {

    private final String raftLeaderHost = "localhost";
    private final int raftLeaderPort = 9001; // Ensure this is the current LEADER

    private final String storageHost = "localhost";
    private final int storagePort = 8080;

    public static void main(String[] args) {
        DistributedClient client = new DistributedClient();

        String filename = "my_vacation.jpg";

        // 1. UPLOAD
        client.uploadFile(filename, "This is the binary data of a purely imaginary image.");

        System.out.println("\n--- Simulating time passing... ---\n");

        // 2. DOWNLOAD
        client.downloadFile(filename);
    }

    // --- HIGH LEVEL WORKFLOWS ---

    public void uploadFile(String filename, String content) {
        System.out.println("--- STARTING UPLOAD: " + filename + " ---");

        // 1. Write Data to Storage Node
        boolean storageSuccess = sendStoragePut(filename, content);

        if (storageSuccess) {
            System.out.println("Data written to Storage Node.");

            // 2. Update Metadata in Raft
            String metadataCommand = "SET " + filename + "=" + storageHost + ":" + storagePort;
            boolean metaSuccess = sendRaftCommand(metadataCommand);

            if (metaSuccess) {
                System.out.println("Metadata updated in Raft Cluster.");
            } else {
                System.out.println("Failed to update Metadata.");
            }
        } else {
            System.out.println("Failed to write to Storage Node.");
        }
    }

    public void downloadFile(String filename) {
        System.out.println("--- STARTING DOWNLOAD: " + filename + " ---");

        // 1. Ask Raft for location
        String location = sendRaftQuery("GET " + filename);
        System.out.println("Raft says file is at: " + location);

        if (location == null || location.startsWith("ERROR") || location.equals("(null)")) {
            System.out.println("File not found in Metadata.");
            return;
        }

        // Parse location (host:port)
        String[] parts = location.split(":");
        String targetHost = parts[0];
        int targetPort = Integer.parseInt(parts[1]);

        // 2. Fetch Data from Storage Node
        String data = fetchFromStorage(targetHost, targetPort, filename);

        if (data != null) {
            System.out.println("RECEIVED DATA: " + data);
            System.out.println("DOWNLOAD COMPLETE!");
        } else {
            System.out.println("Failed to retrieve data from Storage Node.");
        }
    }

    // --- NETTY HELPERS ---

    private boolean sendStoragePut(String key, String data) {
        return (boolean) sendNettyRequest(storageHost, storagePort,
                new NetworkMessage(NetworkMessage.Type.PUT, key, data.getBytes()));
    }

    private String fetchFromStorage(String host, int port, String key) {
        Object result = sendNettyRequest(host, port,
                new NetworkMessage(NetworkMessage.Type.GET, key, null));

        if (result instanceof String) return (String) result;
        return null;
    }

    private boolean sendRaftCommand(String command) {
        RaftMessage msg = new RaftMessage(RaftMessage.Type.CLIENT_COMMAND, "CLIENT", 0);
        msg.setPayload(command);
        Object result = sendNettyRequest(raftLeaderHost, raftLeaderPort, msg);
        return result instanceof Boolean && (Boolean) result;
    }

    private String sendRaftQuery(String query) {
        RaftMessage msg = new RaftMessage(RaftMessage.Type.CLIENT_COMMAND, "CLIENT", 0);
        msg.setPayload(query);
        Object result = sendNettyRequest(raftLeaderHost, raftLeaderPort, msg);
        return (result instanceof String) ? (String) result : null;
    }

    private Object sendNettyRequest(String host, int port, Object request) {
        final Object[] responseContainer = {null};
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new SimpleChannelInboundHandler<Object>() {
                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) {
                                            ctx.writeAndFlush(request);
                                        }
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                            // Handle Storage Messages
                                            if (msg instanceof NetworkMessage) {
                                                NetworkMessage netMsg = (NetworkMessage) msg;
                                                if (netMsg.getType() == NetworkMessage.Type.RESPONSE_OK) {
                                                    if (netMsg.getData() != null) {
                                                        responseContainer[0] = new String(netMsg.getData()); // Return Data
                                                    } else {
                                                        responseContainer[0] = true; // Return Success
                                                    }
                                                }
                                            }
                                            // Handle Raft Messages
                                            else if (msg instanceof RaftMessage) {
                                                RaftMessage raftMsg = (RaftMessage) msg;
                                                if (raftMsg.getPayload() != null && !raftMsg.getPayload().equals("OK")) {
                                                    responseContainer[0] = raftMsg.getPayload(); // Return Query Result
                                                } else {
                                                    responseContainer[0] = raftMsg.isSuccess(); // Return Command Success
                                                }
                                            }
                                            ctx.close();
                                        }
                                        @Override
                                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                            ctx.close();
                                        }
                                    }
                            );
                        }
                    });
            b.connect(host, port).sync().channel().closeFuture().sync();
        } catch (Exception e) {
            System.err.println("Connection Failed to " + host + ":" + port);
        } finally {
            group.shutdownGracefully();
        }
        return responseContainer[0];
    }
}