package com.flipkart.nettyrpc.client;

import com.flipkart.nettyrpc.common.MessageMetadata;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class ClientOutboundHandler extends ChannelOutboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(ClientOutboundHandler.class);
    private final ChannelState channelState;

    public ClientOutboundHandler(ChannelState channelState) {

        this.channelState = channelState;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if(!(msg instanceof Batch)) {
            log.error("msg not instance of Batch {}", msg.getClass());
            promise.setFailure(new Exception("accepting only Batch objects"));
            return;
        }

        Batch batch = (Batch)msg;
        final Map<Long, MessageMetadata> messages = batch.messages;
        long now = System.nanoTime();
        for (MessageMetadata messageMetadata : messages.values()) {
            messageMetadata.deqNanos = now;
        }

        ctx.write(batch.getByteBuf(), promise).addListener((GenericFutureListener<ChannelFuture>) future -> {
            if(future.isSuccess()) {
                channelState.writeSuccess(messages);
            }
            long now1 = System.nanoTime();
            for (MessageMetadata messageMetadata : messages.values()) {
                messageMetadata.sendCompleteNanos = now1;
            }
        });
    }
}
