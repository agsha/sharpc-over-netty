package com.flipkart.nettyrpc.server;

import com.flipkart.nettyrpc.common.MessageParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.flipkart.nettyrpc.server.NettyServer.WRITE_BUF;

class ServerChannelHandler extends ByteToMessageDecoder implements MessageParser.Callback {
    private static final Logger log = LoggerFactory.getLogger(ServerChannelHandler.class);

    private final NettyServer nettyServer;
    private MessageParser messageParser;
    private ChannelHandlerContext ctx;

    public ServerChannelHandler(NettyServer nettyServer) {
        this.nettyServer = nettyServer;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.handlerAdded(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Attribute<ByteBuf> attr = ctx.channel().attr(WRITE_BUF);
        ByteBuf byteBuf = ctx.alloc().buffer(2 * nettyServer.serverBatchSizeBytes);
        attr.set(byteBuf);
        nettyServer.allChannels.add(ctx.channel());
        messageParser = new MessageParser(this, ctx.alloc());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Attribute<ByteBuf> attr = ctx.channel().attr(WRITE_BUF);
        ByteBuf bf = attr.getAndSet(null);
        ReferenceCountUtil.release(bf);
        super.channelInactive(ctx);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (!ctx.channel().isWritable()) {
            log.debug("making channel unwritable {}->{}", ctx.channel().localAddress(), ctx.channel().remoteAddress());
            ctx.channel().config().setAutoRead(false);
        } else {
            log.debug("making channel writable {}->{}", ctx.channel().localAddress(), ctx.channel().remoteAddress());
            ctx.channel().config().setAutoRead(true);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
        messageParser.onData(msg);
    }

    @Override
    public void onMessage(long requestId, ByteBuf msg) {
        nettyServer.requestHandler.onRequest(msg, new ResponseContext(nettyServer, ctx, requestId));

    }

    @Override
    public void onControlMessage(MessageParser.Command command) {
    }

    @Override
    public void onParseError(MessageParser.ParseError error) {

    }
}
