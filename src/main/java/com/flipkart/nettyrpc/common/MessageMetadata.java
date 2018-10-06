package com.flipkart.nettyrpc.common;

import io.netty.buffer.ByteBuf;

import java.nio.channels.CompletionHandler;

public class MessageMetadata<A> {
    public final long id;
    public final CompletionHandler<ByteBuf, A> callback;
    public final A attachment;
    public final long createNanos;
    public long deqNanos;
    public long sendCompleteNanos;
    public long responseNanos;
    public long callbackNanos;

    public MessageMetadata(long id, long createNanos, CompletionHandler<ByteBuf, A> callback, A attachment) {
        this.id = id;
        this.createNanos = createNanos;
        this.callback = callback;
        this.attachment = attachment;
    }
}
