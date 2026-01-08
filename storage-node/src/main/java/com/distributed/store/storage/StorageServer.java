package com.distributed.store.storage;

import com.distributed.store.common.model.RaftMessage;
import com.distributed.store.storage.engine.DiskStorage;
import com.distributed.store.storage.network.StorageHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.bootstrap.Bootstrap; // Client bootstrap for registration
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StorageServer {
    private final int port;
    private final DiskStorage diskStorage;
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();

    // Configuration for "Phoning Home"
    private final String raftLeaderHost = "localhost";
    private final int raftLeaderPort = 9001; // Ideally pass this in args too

    public StorageServer(int port, String dataDir) throws Exception {
        this.port = port;
        this.diskStorage = new DiskStorage(dataDir);
    }

    public void start() throws InterruptedException {
        // 1. Start the Storage Server (Listening for Clients)
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new StorageHandler(diskStorage)
                            );
                        }
                    });

            System.out.println("Storage Node started on port: " + port);
            ChannelFuture serverFuture = bootstrap.bind(port).sync();
            registerWithLeader();
            startHeartbeats();
            serverFuture.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private void startHeartbeats() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            registerWithLeader();
        }, 0, 3, TimeUnit.SECONDS); // Send pulse every 3s
    }

    private void registerWithLeader() {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new SimpleChannelInboundHandler<Object>() {
                                        public void channelActive(ChannelHandlerContext ctx) {
                                            // Send "I am here!" message
                                            String myAddress = "localhost:" + port;
                                            RaftMessage msg = new RaftMessage(RaftMessage.Type.REGISTER_WORKER, "WORKER", 0, myAddress);
                                            ctx.writeAndFlush(msg);
                                            System.out.println("Sent Registration to Leader: " + myAddress);
                                        }
                                        protected void channelRead0(ChannelHandlerContext ctx, Object msg) { ctx.close(); }
                                    });
                        }
                    });
            b.connect(raftLeaderHost, raftLeaderPort).sync();
        } catch (Exception e) {
            System.err.println("Failed to register with Leader (Is Raft Node A running?)");
        } finally {
            group.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = (args.length == 2) ? Integer.parseInt(args[0]) : 8080;
        String dir = (args.length == 2) ? args[1] : "data_node_1";
        new StorageServer(port, dir).start();
    }
}