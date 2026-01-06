package com.distributed.store.metadata;

import com.distributed.store.common.model.RaftMessage;
import com.distributed.store.config.ClusterConfig;
import com.distributed.store.metadata.raft.RaftNode;
import com.distributed.store.metadata.raft.RaftTransport;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MetadataServer {

    // SLF4J Logger - The standard for Java logging
    private static final Logger logger = LoggerFactory.getLogger(MetadataServer.class);

    private final String nodeId;
    private final int port;
    private final RaftNode raftNode;
    private final RaftTransport transport;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public MetadataServer(String nodeId, ClusterConfig config) {
        this.nodeId = nodeId;
        this.port = config.getPort(nodeId);

        // Initialize Transport with topology
        this.transport = new RaftTransport(config.getPeers());

        // Initialize Raft Node logic
        this.raftNode = new RaftNode(nodeId, transport);

        // [CRITICAL] Register the listener for client-side responses
        this.transport.registerHandler(raftNode::handleMessage);
    }

    public void start() throws InterruptedException {
        logger.info("Starting MetadataServer for Node: {}", nodeId);

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(); // Defaults to CPU cores * 2

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    // TUNING: Handle burst connections
                    .option(ChannelOption.SO_BACKLOG, 128)
                    // TUNING: Disable Nagle's algorithm for lower latency (Crucial for Raft)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    // TUNING: Detect dead connections
                    .childOption(ChannelOption.SO_KEEPALIVE, true)

                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    // SECURITY WARNING: ObjectDecoder is fine for learning,
                                    // but use Protobuf/Jackson in real production for security.
                                    new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new RaftServerHandler(raftNode)
                            );
                        }
                    });

            // Bind synchronously
            ChannelFuture f = b.bind(port).sync();
            serverChannel = f.channel();

            logger.info("Server started successfully on port {}.", port);

            // Add Shutdown Hook for graceful termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown hook received. Stopping server...");
                this.stop();
            }));

            // Block until the channel closes
            serverChannel.closeFuture().sync();

        } catch (Exception e) {
            logger.error("Fatal server error", e);
            throw e;
        } finally {
            stop();
        }
    }

    public void stop() {
        logger.info("Stopping components...");
        if (transport != null) {
            transport.stop();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
        logger.info("Server stopped.");
    }

    // --- MAIN ENTRY POINT ---

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java MetadataServer <NodeId> <ConfigFile>");
            System.exit(1);
        }

        String nodeId = args[0];
        String configPath = args[1];

        try {
            // Load Config
            ClusterConfig config = new ClusterConfig(configPath);

            // Start Server
            MetadataServer server = new MetadataServer(nodeId, config);
            server.start();

        } catch (Exception e) {
            logger.error("Failed to start application", e);
            System.exit(1);
        }
    }

    // --- NETTY HANDLER ---

    @ChannelHandler.Sharable // Safe to reuse if stateless
    private static class RaftServerHandler extends SimpleChannelInboundHandler<RaftMessage> {
        private final RaftNode raftNode;

        public RaftServerHandler(RaftNode raftNode) {
            this.raftNode = raftNode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessage msg) {
            if (logger.isDebugEnabled()) {
                logger.debug("Received message: {} from {}", msg.getType(), msg.getSenderId());
            }

            try {
                // Pass to Raft Logic
                RaftMessage response = raftNode.handleMessage(msg);

                // If sync response is returned, write it back
                if (response != null) {
                    ctx.writeAndFlush(response);
                }
            } catch (Exception e) {
                logger.error("Error processing Raft message", e);
                // Optional: Send error response back to peer?
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Netty pipeline exception", cause);
            ctx.close();
        }
    }
}