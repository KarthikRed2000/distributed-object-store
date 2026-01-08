package com.distributed.store.storage.network;

import com.distributed.store.common.model.Chunk;
import com.distributed.store.common.model.NetworkMessage;
import com.distributed.store.storage.engine.DiskStorage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

public class StorageHandler extends SimpleChannelInboundHandler<NetworkMessage> {
    private DiskStorage diskStorage;

    public StorageHandler(DiskStorage diskStorage) {
        this.diskStorage = diskStorage;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NetworkMessage networkMessage) throws Exception {
        try {
            switch (networkMessage.getType()) {
                case PUT:
                    // 1. Write to LOCAL disk
                    Chunk chunk = new Chunk(networkMessage.getKey(), networkMessage.getData());
                    diskStorage.save(chunk);
                    System.out.println(" Wrote locally: " + networkMessage.getKey());

                    // 2. CHECK REPLICATION: Is there a next node?
                    if (networkMessage.getReplicaTarget() != null) {
                        System.out.println(" Forwarding replication to: " + networkMessage.getReplicaTarget());
                        boolean replicaSuccess = replicateToNextNode(networkMessage.getReplicaTarget(), chunk);

                        if (replicaSuccess) {
                            System.out.println(" Replication confirmed by " + networkMessage.getReplicaTarget());
                            channelHandlerContext.writeAndFlush(new NetworkMessage(NetworkMessage.Type.RESPONSE_OK, null, (String) null));
                        } else {
                            System.err.println(" Replication failed.");
                            channelHandlerContext.writeAndFlush(new NetworkMessage(NetworkMessage.Type.RESPONSE_ERROR, null, (String) null));
                        }
                    } else {
                        // No replication needed (I am the last node)
                        channelHandlerContext.writeAndFlush(new NetworkMessage(NetworkMessage.Type.RESPONSE_OK, null, (String) null));
                    }
                    break;
                case GET:
                    System.out.println("Received GET request for: " + networkMessage.getKey());
                    Chunk loadedChunk = diskStorage.load(networkMessage.getKey());

                    channelHandlerContext.writeAndFlush(new NetworkMessage(NetworkMessage.Type.RESPONSE_OK, loadedChunk.getData(), "Found"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            channelHandlerContext.writeAndFlush(new NetworkMessage(NetworkMessage.Type.RESPONSE_ERROR, null, e.getMessage()));
        }
    }

    private boolean replicateToNextNode(String targetAddress, Chunk chunk) {
        String[] parts = targetAddress.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        final boolean[] success = {false};
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new SimpleChannelInboundHandler<NetworkMessage>() {
                                        public void channelActive(ChannelHandlerContext ctx) {
                                            // Send PUT to the next node (with NO further target)
                                            NetworkMessage forwardMsg = new NetworkMessage(NetworkMessage.Type.PUT, chunk.getId(), chunk.getData());
                                            forwardMsg.setReplicaTarget(null); // Stop the chain here
                                            ctx.writeAndFlush(forwardMsg);
                                        }
                                        protected void channelRead0(ChannelHandlerContext ctx, NetworkMessage msg) {
                                            if (msg.getType() == NetworkMessage.Type.RESPONSE_OK) success[0] = true;
                                            ctx.close();
                                        }
                                    });
                        }
                    });
            b.connect(host, port).sync().channel().closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
        return success[0];
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}