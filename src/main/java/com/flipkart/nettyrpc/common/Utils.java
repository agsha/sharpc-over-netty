package com.flipkart.nettyrpc.common;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.CompletionHandler;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static String desc(ChannelHandlerContext ctx) {
        if(ctx == null) {
            return "invalid ctx";
        }
        return desc(ctx.channel());
    }

    public static String desc(Channel channel) {
        if(channel == null) {
            return "invalid channel";
        }
        return String.format("%s->%s", channel.localAddress(), channel.remoteAddress());
    }

    public static String desc(ChannelFuture future) {
        if(future == null) {
            return "invalid future";
        }
        return String.format("%s->%s", future.channel().localAddress(), future.channel().remoteAddress());
    }


    public static long awaitFutureForNanos(long timeoutNanos, io.netty.util.concurrent.Future<?> future) throws InterruptedException {
        long now = System.nanoTime();
        future.await(timeoutNanos, TimeUnit.NANOSECONDS);
        return Math.max(timeoutNanos - (System.nanoTime() - now), 0);
    }

    public static long awaitTerminationBootstrap(long timeoutNanos, Bootstrap bootstrap) throws InterruptedException {
        if(bootstrap != null) {
            return awaitTerminationEventloopGroup(timeoutNanos, bootstrap.config().group());
        }
        return timeoutNanos;
    }

    public static long awaitTerminationEventloopGroup(long timoutNanos, EventLoopGroup eventLoopGroup) throws InterruptedException {
        return awaitTermination(eventLoopGroup, timoutNanos);
    }

    public static long awaitTermination(ExecutorService executor, long timeoutNanos) throws InterruptedException {
        long now = System.nanoTime();
        executor.awaitTermination(timeoutNanos, TimeUnit.NANOSECONDS);
        return Math.max(timeoutNanos - (System.nanoTime() - now), 0);
    }
    public static long awaitTerminationChannelGroup(ChannelGroup allChannels, long timeoutNanos) throws InterruptedException {
        while (allChannels.size() > 0) {
            try {
                if(timeoutNanos <= 0) {
                    break;
                }
                Channel next = allChannels.iterator().next();
                timeoutNanos = Utils.awaitFutureForNanos(timeoutNanos, next.closeFuture());
            } catch (NoSuchElementException e) {
                // we got a race
                // The next iteration should break the loop
            }
        }
        return timeoutNanos;
    }

    public static void safelyFailCallback(CompletionHandler handler, Throwable exc, Object attachment) {
        try {
            handler.failed(exc, attachment);
        } catch (Exception e) {
            log.error("error with callback", e);
        }
    }

    public static void safelyCompleteCallback(CompletionHandler handler, Object result, Object attachment) {
        try {
            handler.completed(result, attachment);
        } catch (Exception e) {
            log.error("error with callback", e);
        }
    }
}
