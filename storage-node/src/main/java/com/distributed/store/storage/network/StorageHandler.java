package com.distributed.store.storage.network;

import com.distributed.store.common.model.Chunk;
import com.distributed.store.common.model.NetworkMessage;
import com.distributed.store.storage.engine.DiskStorage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

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
                    System.out.println("Received PUT request for: " + networkMessage.getKey());
                    Chunk chunk = new Chunk(networkMessage.getKey(), networkMessage.getData());

                    diskStorage.save(chunk);
                    channelHandlerContext.writeAndFlush(new NetworkMessage(NetworkMessage.Type.RESPONSE_OK, null, "Saved"));
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}