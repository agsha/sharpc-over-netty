package com.flipkart.nettyrpc.client;

import com.flipkart.nettyrpc.common.Endpoint;
import com.flipkart.nettyrpc.common.MessageParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.flipkart.nettyrpc.client.NettyTransport.SERVER_CLOSE_ACK;
import static com.flipkart.nettyrpc.common.Utils.desc;

class ClientInboundHandler extends ByteToMessageDecoder implements MessageParser.Callback {
    private static final Logger log = LoggerFactory.getLogger(ClientInboundHandler.class);

    private final ChannelState channelState;
    // this is a global variable shared by all channels.
    // whenever a channel becomes active, it is added to this group.
    // the removal is automatic and taken care by the ChannelGroup itself
    private final ChannelGroup allChannels;
    private final Endpoint endpoint;
    private final NettyTransport.ClientEventHandler handler;
    private final int cleanupIntervalSeconds;
    private final int metricDumpSecs;
    private ScheduledExecutorService executor;
    private MessageParser messageParser;
    private ChannelHandlerContext ctx;
    private ScheduledFuture<?> cleanupTask;
    private ScheduledFuture<?> metricsDumper;

    public ClientInboundHandler(ChannelState channelState, ChannelGroup allChannels, Endpoint endpoint, NettyTransport.ClientEventHandler handler, int cleanupIntervalSeconds, int metricDumpSecs, ScheduledExecutorService executor) {
        this.channelState = channelState;
        this.allChannels = allChannels;
        this.endpoint = endpoint;
        this.handler = handler;
        this.cleanupIntervalSeconds = cleanupIntervalSeconds;
        this.metricDumpSecs = metricDumpSecs;
        this.executor = executor;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
        // apparently netty calls decode even from the channelInactive event
        if(!ctx.channel().isActive()) {
            return;
        }
        channelState.updateCurrentIterationTime();
        messageParser.onData(msg);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        cleanupTask = ctx.channel().eventLoop().scheduleAtFixedRate(() -> channelState.cleanup(ctx), cleanupIntervalSeconds, cleanupIntervalSeconds, TimeUnit.SECONDS);
        metricsDumper = ctx.channel().eventLoop().scheduleAtFixedRate(channelState::dumpMetrics, metricDumpSecs, metricDumpSecs, TimeUnit.SECONDS);
        allChannels.add(ctx.channel());
        channelState.addGaugeForPendingResponses(desc(ctx));
        this.messageParser = new MessageParser(this, ctx.alloc());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cleanupTask.cancel(false);
        metricsDumper.cancel(false);
        channelState.failAll();
        channelState.removeGaugeForPendingResponses();
        super.channelInactive(ctx);
        // to prevent an annoying exception which would occur during shutdown time
        if(!executor.isShutdown()) {
            executor.schedule(() -> handler.onChannelInactive(endpoint), 0, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onMessage(long requestId, ByteBuf msg) {
        channelState.onMessage(ctx, requestId, msg);
    }

    @Override
    public void onControlMessage(MessageParser.Command command) {
        if(command == MessageParser.Command.CLOSE) {
            log.info("received CLOSE command from server {}", desc(ctx));
            executor.schedule(() -> handler.onServerCloseEvent(endpoint, ctx), 0, TimeUnit.SECONDS);
        } else {
            log.error("unsupported command from server {}", command);
        }
    }

    @Override
    public void onParseError(MessageParser.ParseError error) {
        log.error("{} {}", error.description, error.object);
        ctx.channel().close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if(evt instanceof String) {
            String event = (String)evt;
            if(event.equals(SERVER_CLOSE_ACK)) {
                log.info("eventloop got ack from client {}", desc(ctx));
                channelState.setClosing(ctx, true);
            }
        }
    }
}
