package com.distributed.store.metadata;

import com.distributed.store.common.model.RaftMessage;
import com.distributed.store.metadata.raft.RaftNode;
import com.distributed.store.metadata.raft.RaftTransport;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.util.Arrays;
import java.util.List;

public class MetadataServer {
    private final int port;
    private final RaftNode raftNode;

    public MetadataServer(int port, String nodeId, List<String> peers) {
        this.port = port;

        RaftTransport transport = new RaftTransport(peers);
        this.raftNode = new RaftNode(nodeId, transport);
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new SimpleChannelInboundHandler<RaftMessage>() {

                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) throws Exception {
                                            RaftMessage response = raftNode.handleMessage(msg);
                                            if (response != null) {
                                                ctx.writeAndFlush(response);
                                            }
                                        }
                                    }
                                );
                            }
                        });
            System.out.println("Metadata Node " + raftNode + " started on port " + port);
            bootstrap.bind(port).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: <port> <nodeId> <peer1> <peer2>...");
            return;
        }
        int port = Integer.parseInt(args[0]);
        String nodeId = args[1];
        List<String> peers = Arrays.asList(Arrays.copyOfRange(args, 2, args.length));

        new MetadataServer(port, nodeId, peers).start();
    }
}