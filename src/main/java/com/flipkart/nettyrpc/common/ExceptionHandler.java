package com.flipkart.nettyrpc.common;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(GenericErrorHandler.class);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof ReadTimeoutException) {
            log.error("Closing idle connection {}->{}", ctx.channel().localAddress(), ctx.channel().remoteAddress());
        } else {
            log.error("exception occurred in {}->{}", ctx.channel().localAddress(), ctx.channel().remoteAddress(), cause);
        }
        ctx.close();
    }
}
