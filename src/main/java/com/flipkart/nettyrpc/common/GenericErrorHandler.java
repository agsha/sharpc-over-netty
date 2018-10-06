package com.flipkart.nettyrpc.common;

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericErrorHandler implements GenericFutureListener<ChannelFuture> {
    private static final Logger log = LoggerFactory.getLogger(GenericErrorHandler.class);

    public static final GenericErrorHandler instance = new GenericErrorHandler();

    @Override
    public void operationComplete(ChannelFuture future) {
        if(!future.isSuccess()) {
            log.debug("error in operation in channel {}->{}",future.channel().localAddress(), future.channel().remoteAddress(), future.cause());
            future.channel().close();
        }
    }
}
