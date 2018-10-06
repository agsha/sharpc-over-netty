package com.flipkart.nettyrpc.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use this class to send a response back to the client.
 * It is mandatory to call sendResponse for every received request.
 * since it marks the completion of the request.
 */
public class ResponseContext {
    private static final Logger log = LoggerFactory.getLogger(ResponseContext.class);

    private final NettyServer server;
    private final ChannelHandlerContext ctx;
    private final long requestId;
    private boolean sentResponse = false;

    ResponseContext(NettyServer server, ChannelHandlerContext ctx, long requestId) {
        this.server = server;
        this.ctx = ctx;
        this.requestId = requestId;
    }

    public ByteBuf alloc(int capacity) {
        // this is thread safe
        return ctx.alloc().buffer(capacity);
    }

    public ByteBuf alloc() {
        // this is thread safe
        return ctx.alloc().buffer();
    }

    public void sendResponse(ByteBuf response) {
        synchronized (server) {
            if (sentResponse) {
                throw new RuntimeException("response has already been sent");
            }
            server.sendResponse(ctx, response, requestId);
            sentResponse = true;
        }
    }

    void flush() {
        server.writeAndFlush(ctx);
    }
}
