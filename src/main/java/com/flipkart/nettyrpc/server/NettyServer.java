package com.flipkart.nettyrpc.server;

import com.flipkart.nettyrpc.common.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.flipkart.nettyrpc.common.ConfigKey.*;
import static com.flipkart.nettyrpc.common.Utils.desc;

/**
 * Accepts a @RequestHandler which is invoked when a request is received by the server.
 * The requestHandler gets passed both the request object and a @ResponseContext which can
 * be used to send responses.
 * Response to the request can be sent by calling @ResponseContext#sendResponse()
 * It is mandatory that every request is responded with a call to @ResponseContext#sendResponse()
 *
 * The request object will be garbage collected once the requestHandler invocation is completed.
 * If it is necessary to hold on to the request object even after the invocation is complete,
 * then you can call @ReferenceCountUtil.retain(request) and release it when you're done.
 * The second option is to simply make a copy the request to your own data structure
 *
 */
public class NettyServer {
    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);
    private static final String HANDLER_NAME = "serverHandler";
    final RequestHandler requestHandler;
    final ChannelGroup allChannels;

    final static AttributeKey<ByteBuf> WRITE_BUF = AttributeKey.valueOf("write_buf");
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final java.util.concurrent.ScheduledFuture<?> flusher;
    private final ChannelFuture serverChannelFuture;
    private final ScheduledExecutorService executor;
    private final DefaultEventExecutor eventExecutor;
    private final NettyConfig nettyConfig;
    public final int serverBatchSizeBytes;

    public NettyServer(RequestHandler requestHandler, Endpoint endpoint) throws InterruptedException {
        this(requestHandler, endpoint, new HashMap<>());
    }
    public NettyServer(RequestHandler requestHandler, Endpoint endpoint, Map<String, String> config) throws InterruptedException {
        this.requestHandler = requestHandler;
        nettyConfig = new NettyConfig(config);
        serverBatchSizeBytes = nettyConfig.getInt(SERVER_BATCH_SIZE_BYTES);
        executor = Executors.newSingleThreadScheduledExecutor();
        eventExecutor = new DefaultEventExecutor();
        allChannels =
                new DefaultChannelGroup(eventExecutor);
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(nettyConfig.getInt(SERVER_LOW_WRITE_WATERMARK_BYTES), nettyConfig.getInt(SERVER_HIGH_WRITE_WATERMARK_BYTES)))
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel ch) {
                        ChannelPipeline p = ch.pipeline();
                        //p.addLast(new LoggingHandler(LogLevel.INFO));
                        p.addLast("readTimeoutHandler", new ReadTimeoutHandler(nettyConfig.getInt(SERVER_READ_TIMEOUT_SECS)));
                        p.addLast("serverHandler", new ServerChannelHandler(NettyServer.this));
                        p.addLast(new ExceptionHandler());
                    }
                });


        if (endpoint instanceof TcpEndpoint) {
            TcpEndpoint tcpEndpoint = (TcpEndpoint)endpoint;
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup(1);
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class);
            serverChannelFuture = bootstrap.bind(tcpEndpoint.port).sync();
        } else if (endpoint instanceof UdsEndpoint) {
            UdsEndpoint udsEndpoint = (UdsEndpoint)endpoint;

            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup(1);
            bootstrap.group(bossGroup, workerGroup)
                    .channel(EpollServerDomainSocketChannel.class);
            serverChannelFuture = bootstrap.bind(new DomainSocketAddress(udsEndpoint.udsFile)).sync();
        } else {
            throw new IllegalStateException("Only TCP and UDS endpoints are supported at this time.");
        }


        flusher = executor.scheduleAtFixedRate(() -> {
            try {
                for (Channel ch : allChannels) {
                    ChannelHandlerContext ctx = ch.pipeline().context(HANDLER_NAME);
                    if (ctx == null) {
                        log.warn("could not find context for channel: {}->{}", ch.localAddress(), ch.remoteAddress());
                        continue;
                    }
                    NettyServer.this.writeAndFlush(ctx);
                }
            } catch (Exception e) {
                log.error("exception in scheduled executor", e);
            }
        }, 5, nettyConfig.getInt(SERVER_LINGER_MS), TimeUnit.MILLISECONDS);

    }

    synchronized void sendResponse(ChannelHandlerContext ctx, ByteBuf response, long requestId) {
        Attribute<ByteBuf> attr = ctx.channel().attr(WRITE_BUF);
        // this was set by the channelActive callback
        ByteBuf byteBuf = attr.get();
        if(byteBuf == null) {
            log.warn("Cannot send the response. the channel has been closed {}->{}", ctx.channel().localAddress(), ctx.channel().remoteAddress());
            return;
        }
        MessageParser.writeMessage(byteBuf, requestId, response);
        ReferenceCountUtil.release(response);
        if (byteBuf.readableBytes() > serverBatchSizeBytes) {
            writeAndFlush(ctx, byteBuf, attr);
        }
    }

    synchronized void writeAndFlush(ChannelHandlerContext ctx) {
        Attribute<ByteBuf> attr = ctx.channel().attr(WRITE_BUF);
        ByteBuf byteBuf = attr.get();
        // add a test case for this code path
        if(byteBuf == null) {
            log.warn("Cannot send the response. the channel has been closed {}->{}", ctx.channel().localAddress(), ctx.channel().remoteAddress());
            return;
        }

        if(byteBuf.readableBytes() == 0) {
            return;
        }
        writeAndFlush(ctx, byteBuf, attr).addListener(GenericErrorHandler.instance);
    }

    private synchronized ChannelFuture writeAndFlush(ChannelHandlerContext ctx, ByteBuf byteBuf, Attribute<ByteBuf> attr) {
        ChannelFuture channelFuture = ctx.channel().writeAndFlush(byteBuf).addListener(GenericErrorHandler.instance);

        // byteBuf will be will be released by the write call
        byteBuf = ctx.alloc().buffer(2 * serverBatchSizeBytes);
        attr.set(byteBuf);
        return channelFuture;
    }

    public synchronized void shutdownGracefully() {
        log.info("initiating server shutdown {}", desc(serverChannelFuture));
        bossGroup.shutdownGracefully(0, 15, TimeUnit.SECONDS).addListener(future -> {
            if(future.isSuccess()) {
                allChannels.writeAndFlush(MessageParser.getControlMessage(MessageParser.Command.CLOSE));
                if(allChannels.size() == 0) {
                    afterChannelsClosed();
                } else {
                    allChannels.newCloseFuture()
                            .addListener(f -> afterChannelsClosed());
                }
            }
        });
    }

    private void afterChannelsClosed() {
        flusher.cancel(false);
        workerGroup.shutdownGracefully(0, 15, TimeUnit.SECONDS);
        executor.shutdown();
        eventExecutor.shutdownGracefully(0, 15, TimeUnit.SECONDS);
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);
        timeoutNanos = Utils.awaitTerminationEventloopGroup(timeoutNanos, bossGroup);
        timeoutNanos = Utils.awaitTerminationChannelGroup(allChannels, timeoutNanos);
        Utils.awaitTermination(eventExecutor, timeoutNanos);
        timeoutNanos = Utils.awaitTermination(executor, timeoutNanos);
        timeoutNanos = Utils.awaitTerminationEventloopGroup(timeoutNanos, workerGroup);
        return timeoutNanos > 0;
    }
}
