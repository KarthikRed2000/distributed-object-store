package com.distributed.store.client;

import com.distributed.store.common.model.NetworkMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.*;

public class SimpleClient {
    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new SimpleChannelInboundHandler<NetworkMessage>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, NetworkMessage msg) {
                                            System.out.println("Server Response: " + msg.getMessage());
                                        }
                                    }
                            );
                        }
                    });

            // Connect to localhost:8080
            Channel channel = b.connect("localhost", 8080).sync().channel();

            // Send a PUT request
            String text = "Netty is fast!";
            NetworkMessage request = new NetworkMessage(
                    NetworkMessage.Type.PUT,
                    "network_chunk_1",
                    text.getBytes()
            );

            System.out.println("Sending data...");
            channel.writeAndFlush(request);

            // Keep connection open briefly to receive response
            Thread.sleep(1000);
            channel.close();

        } finally {
            group.shutdownGracefully();
        }
    }
}