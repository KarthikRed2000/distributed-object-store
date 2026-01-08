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
    private final int raftLeaderPort = 9001;

    public static void main(String[] args) {
        DistributedClient client = new DistributedClient();

        System.out.println("===  STARTING HA CLIENT ===\n");

        // 1. Upload (Creates Primary + Replica)
        client.uploadFile("important_doc1.txt", "This data must survive a crash!");

        System.out.println("\n---  CHAOS TEST: YOU CAN KILL NODE 8080 NOW ---");
        System.out.println("Waiting 10 seconds for you to crash the primary node...");
        try { Thread.sleep(10000); } catch (InterruptedException e) {}

        // 2. Download (Should fail on 8080, then auto-switch to 8081)
        client.downloadFile("important_doc1.txt");
    }

    public void uploadFile(String filename, String content) {
        System.out.println(" UPLOAD: " + filename);

        // 1. ALLOCATE
        String allocation = sendRaftCommand("ALLOCATE");
        if (allocation == null || allocation.startsWith("ERROR")) {
            System.out.println(" Allocation Failed");
            return;
        }

        System.out.println("   🔗 Chain: " + allocation);
        String[] nodes = allocation.split(",");
        String primary = nodes[0];
        String replica = (nodes.length > 1) ? nodes[1] : null;

        String[] primaryParts = primary.split(":");
        NetworkMessage putMsg = new NetworkMessage(NetworkMessage.Type.PUT, filename, content.getBytes());
        if (replica != null) putMsg.setReplicaTarget(replica);

        // 2. WRITE
        Object response = sendNettyRequest(primaryParts[0], Integer.parseInt(primaryParts[1]), putMsg);

        if (response instanceof Boolean && (Boolean) response) {
            System.out.println(" Chain Write Success.");

            // 3. SAVE FULL CHAIN (Primary,Replica)
            // We save the EXACT string Raft gave us (e.g., "localhost:8080,localhost:8081")
            String metadataCommand = "SET " + filename + "=" + allocation;
            sendRaftCommand(metadataCommand);
            System.out.println(" Metadata Saved: " + allocation);
        } else {
            System.out.println(" Write Failed.");
        }
        System.out.println("------------------------------------------");
    }

    public void downloadFile(String filename) {
        System.out.println(" DOWNLOAD: " + filename);

        // 1. GET METADATA
        String locationList = sendRaftCommand("GET " + filename);
        if (locationList == null || locationList.startsWith("ERROR") || locationList.equals("(null)")) {
            System.out.println(" File not found.");
            return;
        }

        System.out.println(" Locations: " + locationList);

        // 2. FAILOVER LOOP
        String[] nodes = locationList.split(",");
        String data = null;

        for (String nodeAddr : nodes) {
            System.out.print("   Trying " + nodeAddr + "... ");
            String[] parts = nodeAddr.split(":");
            data = fetchFromStorage(parts[0], Integer.parseInt(parts[1]), filename);

            if (data != null) {
                System.out.println(" SUCCESS!");
                System.out.println(" CONTENT: [" + data + "]");
                return; // Exit as soon as we get the file
            } else {
                System.out.println(" FAILED/UNREACHABLE");
            }
        }

        System.out.println(" All replicas failed. Data is unavailable.");
        System.out.println("------------------------------------------");
    }

    // --- HELPERS (Unchanged) ---
    private String fetchFromStorage(String host, int port, String key) {
        NetworkMessage request = new NetworkMessage(NetworkMessage.Type.GET, key, null);
        Object response = sendNettyRequest(host, port, request);
        return (response instanceof String) ? (String) response : null;
    }
    private String sendRaftCommand(String command) {
        RaftMessage msg = new RaftMessage(RaftMessage.Type.CLIENT_COMMAND, "CLIENT", 0, command);
        Object response = sendNettyRequest(raftLeaderHost, raftLeaderPort, msg);
        return (response instanceof String) ? (String) response : null;
    }
    private Object sendNettyRequest(String host, int port, Object request) {
        final Object[] responseContainer = {null};
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new ObjectEncoder(), new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                            new SimpleChannelInboundHandler<Object>() {
                                public void channelActive(ChannelHandlerContext ctx) { ctx.writeAndFlush(request); }
                                protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                    if (msg instanceof NetworkMessage) {
                                        NetworkMessage nm = (NetworkMessage) msg;
                                        responseContainer[0] = (nm.getType() == NetworkMessage.Type.RESPONSE_OK) ?
                                                ((nm.getData() != null) ? new String(nm.getData()) : true) : null;
                                    } else if (msg instanceof RaftMessage) {
                                        RaftMessage rm = (RaftMessage) msg;
                                        responseContainer[0] = (rm.getPayload() != null) ? rm.getPayload() : rm.isSuccess();
                                    }
                                    ctx.close();
                                }
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }
                            });
                }
            });
            b.connect(host, port).sync().channel().closeFuture().sync();
        } catch (Exception e) {
            // Return null on connection failure
        } finally { group.shutdownGracefully(); }
        return responseContainer[0];
    }
}