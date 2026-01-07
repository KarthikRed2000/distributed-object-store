package com.distributed.store.client;

import com.distributed.store.common.model.RaftMessage;
import com.distributed.store.common.model.RaftMessage.Type;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.*;

import java.util.Scanner;

public class ClusterClient {

    public static void main(String[] args) {
        // Connect to Node_A by default (Leader usually emerges there or we get redirected)
        String host = "localhost";
        int port = 9001;

        System.out.println("Connecting to " + host + ":" + port);

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
                                    new ClientHandler()
                            );
                        }
                    });

            Channel channel = b.connect(host, port).sync().channel();

            // Command Loop
            Scanner scanner = new Scanner(System.in);
            System.out.println("Type a command (e.g., 'SET user=admin') or 'exit':");

            while(true) {
                System.out.print("> ");
                String input = scanner.nextLine();
                if ("exit".equalsIgnoreCase(input)) break;

                // Create Command Message
                RaftMessage cmdMsg = new RaftMessage(Type.CLIENT_COMMAND, "Client", 0, 0, 0, 0);
                cmdMsg.setPayload(input); // Ensure you added this field to RaftMessage!

                channel.writeAndFlush(cmdMsg);
            }

            channel.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }

    private static class ClientHandler extends SimpleChannelInboundHandler<RaftMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) {
            if (msg.getType() == Type.CLIENT_COMMAND) {
                if (msg.isSuccess()) {
                    System.out.println("[SUCCESS] Leader committed the command.");
                } else {
                    System.out.println("[FAIL] Node is not Leader or replication failed.");
                }
            }
        }
    }
}