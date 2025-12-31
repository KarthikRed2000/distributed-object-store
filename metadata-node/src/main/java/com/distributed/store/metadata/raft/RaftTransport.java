package com.distributed.store.metadata.raft;

import com.distributed.store.common.model.RaftMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RaftTransport {
    private final List<String> peerAddresses;
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(4);
    private final Map<String, Channel> connectionCache = new ConcurrentHashMap<>();

    public RaftTransport(List<String> peerAddresses) {
        this.peerAddresses = peerAddresses;
    }

    public void broadcast(RaftMessage message) {
        for (String peer : peerAddresses) {
            sendToPeer(peer, message);
        }
    }

    private void sendToPeer(String address, RaftMessage message) {
        String[] parts = address.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        new Thread(() -> {
            EventLoopGroup group = new NioEventLoopGroup();
            try{
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(
                                        new ObjectEncoder(),
                                        new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                        new SimpleChannelInboundHandler<RaftMessage>() {
                                            @Override
                                            protected void channelRead0(ChannelHandlerContext channelHandlerContext, RaftMessage raftMessage) throws Exception {
                                                //
                                            }
                                        }
                                );
                            }
                        });
                ChannelFuture future = bootstrap.connect(host, port).sync();
                future.channel().writeAndFlush(message);
                future.channel().closeFuture().sync();
            } catch (Exception e){
                System.err.println("Failed to send to " + address + ": " + e.getMessage());
            } finally {
                group.shutdownGracefully();
            }
        }).start();
    }
}
