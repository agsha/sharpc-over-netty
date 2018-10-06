package com.flipkart.nettyrpc.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.nettyrpc.common.*;
import com.flipkart.nettyrpc.common.exceptions.ServerShuttingDownException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.flipkart.nettyrpc.common.Utils.desc;

/**
 * Exposes APIs to connect to a remote endpoint, send and receive data
 *
 * This class is not thread-safe
 */
class NettyTransport {
    private static final Logger log = LoggerFactory.getLogger(NettyTransport.class);
    private final ClientEventHandler handler;
    private final int cleanupIntervalSeconds;
    private final int readTimeoutSecs;
    private final int metricDumpSecs;
    private final MetricRegistry metrics;
    private final ScheduledExecutorService executor;

    private final ChannelGroup allChannels;


    private Bootstrap tcpBootstrap;
    private Bootstrap udsBootstrap;
    public static final String SERVER_CLOSE_ACK = "SERVER_CLOSE_ACK";
    private final DefaultEventExecutor eventExecutor;

    public NettyTransport(boolean needTcp, boolean needUds, ClientEventHandler handler, int cleanupIntervalSeconds, int readTimeoutSecs, int metricDumpSecs, MetricRegistry metrics, ScheduledExecutorService executor) {
        this.handler = handler;
        this.cleanupIntervalSeconds = cleanupIntervalSeconds;
        this.readTimeoutSecs = readTimeoutSecs;
        this.metricDumpSecs = metricDumpSecs;
        this.metrics = metrics;
        this.executor = executor;
        eventExecutor = new DefaultEventExecutor();
        allChannels = new DefaultChannelGroup(eventExecutor);
        if (needTcp) {
            EventLoopGroup group = new NioEventLoopGroup(1);
            tcpBootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true);
        }
        if (needUds) {
            EventLoopGroup group = new EpollEventLoopGroup(1);
            udsBootstrap = new Bootstrap()
                    .group(group)
                    .channel(EpollDomainSocketChannel.class);
        }
    }

    /**
     * Connects to an endpoint and blocks until the connection is made.
     */
    public void connectSync(Endpoint endpoint) {
        ChannelFuture future = null;
        if (endpoint instanceof TcpEndpoint) {
            Bootstrap b = tcpBootstrap;
            TcpEndpoint tcp = (TcpEndpoint) endpoint;
            future = configure(b, endpoint).connect(tcp.host, tcp.port).addListener(GenericErrorHandler.instance);
        } else if (endpoint instanceof UdsEndpoint) {
            Bootstrap b = udsBootstrap;
            UdsEndpoint udsEndpoint = (UdsEndpoint) endpoint;
            future = configure(b, endpoint).connect(new DomainSocketAddress(udsEndpoint.udsFile)).addListener(GenericErrorHandler.instance);
        }
        try {
            future.sync();
        } catch (Exception e) {
            log.error("connection to endpoint {} failed due to {}", endpoint, e.getMessage());
            log.debug("connection to endpoint {} failed due to ", endpoint, e); // print full stacktrace in debug mode
            endpoint.connectFailureCause = e;
            return;
        }
        log.info("connection was successful {}", desc(future.channel()));

        endpoint.channel = future.channel();
    }

    private Bootstrap configure(Bootstrap bootstrap, final Endpoint endpoint) {
        return bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(Channel ch) {
                ChannelPipeline p = ch.pipeline();
                ChannelState channelState = new ChannelState(cleanupIntervalSeconds * 1_000_000_000L, metrics);
                //p.addLast(new LoggingHandler(LogLevel.INFO));
                p.addLast(new ReadTimeoutHandler(readTimeoutSecs));
                p.addLast(new ClientOutboundHandler(channelState));
                p.addLast(new ClientInboundHandler(channelState, allChannels, endpoint, handler, cleanupIntervalSeconds, metricDumpSecs, executor));
                p.addLast(new ExceptionHandler());
            }
        });
    }

    /**
     * returns true if able to write and flush successfully to the endpoint
     */
    public boolean connectWriteFlushSync(Batch batch, final Endpoint endpoint) throws InterruptedException, ServerShuttingDownException {
        if (!endpoint.serverUp) {
            log.debug("unable to send because server has indicated it is shutting down");
            ReferenceCountUtil.release(batch.getByteBuf());
            throw new ServerShuttingDownException();
        }
        connectSyncIfTime(endpoint);
        if (endpoint.channel == null || !endpoint.channel.isActive()) {
            ReferenceCountUtil.release(batch.getByteBuf());
            return false;
        }

        // channel will not be null at this time
        endpoint.channel.writeAndFlush(batch).sync();
        return true;
    }

    /**
     * Checks if the last connect attempt was not "recent enough" and if not,
     * then attempts to connect to the endpoint
     */
    private void connectSyncIfTime(final Endpoint endpoint) {
        long now = System.nanoTime();
        Channel localChannel = endpoint.channel;
        boolean disconnected = (localChannel == null || !localChannel.isActive());
        if (disconnected && now - endpoint.lastReconnectAttemptNanos > endpoint.reconnectWaitNanos) {
            log.info("attempting reconnect to {}", endpoint);
            endpoint.lastReconnectAttemptNanos = now;
            connectSync(endpoint);
        }
    }

    public void shutdown(Runnable todoAfterChannelsClosed) {
        for (Channel channel : allChannels) {
            channel.pipeline().fireUserEventTriggered(NettyTransport.SERVER_CLOSE_ACK);
        }
        if(allChannels.size() > 0) {
            allChannels.newCloseFuture().addListener(f -> {
                afterChannelsClosed(todoAfterChannelsClosed);
            });
        } else {
            afterChannelsClosed(todoAfterChannelsClosed);
        }
    }

    private void afterChannelsClosed(Runnable todoAfterCloseChannel) {
        if(tcpBootstrap != null) {
            tcpBootstrap.config().group().shutdownGracefully(0, 15, TimeUnit.SECONDS);
        }
        if(udsBootstrap!=null) {
            udsBootstrap.config().group().shutdownGracefully(0, 15, TimeUnit.SECONDS);
        }
        if(todoAfterCloseChannel!=null) {
            todoAfterCloseChannel.run();
        }
        eventExecutor.shutdownGracefully(0, 15, TimeUnit.SECONDS);
    }

    public void shutdown() {
        shutdown(null);
    }

    public long awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);
        timeoutNanos = Utils.awaitTerminationChannelGroup(allChannels, timeoutNanos);
        timeoutNanos = Utils.awaitTermination(eventExecutor, timeoutNanos);
        return awaitTerminationEventloopGroups(timeoutNanos);
    }

    private long awaitTerminationEventloopGroups(long timeoutNanos) throws InterruptedException {
        timeoutNanos = Utils.awaitTerminationBootstrap(timeoutNanos, tcpBootstrap);
        return Utils.awaitTerminationBootstrap(timeoutNanos, udsBootstrap);
    }

    interface ClientEventHandler {
        void onServerCloseEvent(Endpoint endpoint, ChannelHandlerContext ctx);
        void onChannelInactive(Endpoint endpoint);
    }
}
