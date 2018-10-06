package com.flipkart.nettyrpc.server;

import io.netty.buffer.ByteBuf;

public interface RequestHandler {
    void onRequest(ByteBuf request, ResponseContext ctx);
}
