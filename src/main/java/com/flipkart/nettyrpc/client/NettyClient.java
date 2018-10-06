package com.flipkart.nettyrpc.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.nettyrpc.common.*;
import com.flipkart.nettyrpc.common.exceptions.NoEndpointAvailable;
import com.flipkart.nettyrpc.common.exceptions.ServerShuttingDownException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.CompletionHandler;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.flipkart.nettyrpc.common.Utils.desc;

/**
 * This client can be configured with a primary and sideline servers
 * First, the client tries to send to primary endpoint. If the send fails,
 * then it picks a random sideline endpoint and tries to send to that.
 * If that also fails, the message is marked as failed and the fail-callback is fired.
 *
 * The response callback is fired from the eventloop thread so it is important to
 * not block in the response callback.
 *
 * This class is thread-safe
 */
public class NettyClient implements NettyTransport.ClientEventHandler {
    private static final Logger log = LoggerFactory.getLogger(NettyClient.class);

    private Batch batch;
    private final AtomicLong requestId = new AtomicLong(0);
    private final NettyTransport transport;
    private final List<Endpoint> endpoints;
    private final ByteBufAllocator allocator;
    private final java.util.concurrent.ScheduledFuture<?> periodicFlusher;
    private boolean closed;
    private ScheduledExecutorService executor;
    private int batchSizeBytes;
    private NoEndpointAvailable noEndpointAvailableException;

    public NettyClient(Endpoint primary, List<Endpoint> endpoints) {
        this(primary, endpoints, new MetricRegistry(), Executors.newSingleThreadScheduledExecutor(), new HashMap<>());
    }

    public NettyClient(Endpoint primary, List<Endpoint> endpoints, Map<String, String> config) {
        this(primary, endpoints, new MetricRegistry(), Executors.newSingleThreadScheduledExecutor(), config);
    }

    public NettyClient(Endpoint primary, List<Endpoint> endpoints, Map<String, String> config, MetricRegistry registry) {
        this(primary, endpoints, registry, Executors.newSingleThreadScheduledExecutor(), config);
    }


    public NettyClient(Endpoint primary, List<Endpoint> fallback, MetricRegistry metrics, ScheduledExecutorService executor, Map<String, String> configMap) {
        NettyConfig config = new NettyConfig(configMap);
        batchSizeBytes = config.getInt(ConfigKey.CLIENT_BATCH_SIZE_BYTES);
        int expiryIntervalSecs = config.getInt(ConfigKey.CLIENT_EXPIRY_INTERVAL_SECS);
        int readTimeoutSecs = config.getInt(ConfigKey.CLIENT_READ_TIMEOUT_SECS);
        int metricDumpSecs = config.getInt(ConfigKey.CLIENT_METRIC_DUMP_SECS);
        int lingerMs = config.getInt(ConfigKey.CLIENT_LINGER_MS);

        this.executor = executor;
        Collections.shuffle(fallback);
        this.endpoints = new ArrayList<>();
        this.endpoints.add(primary);
        this.endpoints.addAll(fallback);
        log.info("The shuffled list of endpoints to try (The first one is primary endpoint) is {}", endpoints);
        Metrics.getInstance().init(metrics, new int[]{50, 90, 95, 99}, executor);
        allocator = new PooledByteBufAllocator();
        batch = new Batch(allocator.buffer(batchSizeBytes), batchSizeBytes);
        boolean needTcp = false;
        boolean needUds = false;
        for (Endpoint endpoint : endpoints) {
            if(endpoint instanceof TcpEndpoint) {
                needTcp = true;
            } else if(endpoint instanceof UdsEndpoint) {
                needUds = true;
            }
        }
        noEndpointAvailableException = new NoEndpointAvailable();
        transport = new NettyTransport(needTcp, needUds, this, expiryIntervalSecs, readTimeoutSecs, metricDumpSecs, metrics, executor);

        periodicFlusher = executor.scheduleAtFixedRate(() -> {
            try {
                connectWriteAndFlushSync();
            } catch (Exception e) {
                log.error("exception in client scheduled flusher", e);
            }
        }, 5, lingerMs, TimeUnit.MILLISECONDS);
    }

    public synchronized <A> void send(byte[] data, CompletionHandler<ByteBuf, A> callback, A attachment) {
        throwIfClosed();
        MessageMetadata messageMetadata = new MessageMetadata<>(requestId.incrementAndGet(), System.nanoTime(), callback, attachment);
        boolean full = batch.addMessage(messageMetadata, data);
        if (full) {
            connectWriteAndFlushSync();
        }
    }

    public synchronized void connectSync(Endpoint endpoint) {
        throwIfClosed();
        transport.connectSync(endpoint);
    }

    private synchronized void close(Endpoint endpoint) {
        if (endpoint.channel != null) {
            endpoint.channel.close().addListener(GenericErrorHandler.instance);
        }
    }

    private synchronized void connectWriteAndFlushSync() {
        boolean success = false;

        for (Endpoint endpoint : endpoints) {
            if (batch.getByteBuf().readableBytes() == 0) {
                success = true;
                break;
            }
            if (closed) {
                log.warn("write called on closed client");
                return;
            }

            try {
                success = connectWriteAndFlushSync(endpoint);
                if(success) {
                    break;
                }
            } catch (Exception e) {
                log.debug("exception trying to send to endpoint ", e);
                if (!(e instanceof ServerShuttingDownException)) {
                    // if server is shutting down it will be closed automatically after all
                    // responses have been read
                    close(endpoint);
                }
            }
        }

        if(!success) {
            batch.failBatch(noEndpointAvailableException);
        }
        releaseBatch();
    }

    private void releaseBatch() {
        ReferenceCountUtil.release(batch.getByteBuf());
        if(ReferenceCountUtil.refCnt(batch.getByteBuf()) != 0) {
            throw new RuntimeException(String.format("batch ref count must be 0 but it is %s", ReferenceCountUtil.refCnt(batch.getByteBuf())));
        }
        batch = new Batch(allocator.buffer(batchSizeBytes), batchSizeBytes);
    }


    private boolean connectWriteAndFlushSync(Endpoint endpoint) throws InterruptedException, ServerShuttingDownException {
        ReferenceCountUtil.retain(batch.getByteBuf());
        return transport.connectWriteFlushSync(batch, endpoint);
    }

    @Override
    public synchronized void onServerCloseEvent(Endpoint endpoint, ChannelHandlerContext ctx) {
        throwIfClosed();
        log.info("client got CLOSE command from eventLoop {}", desc(ctx));
        endpoint.serverUp = false;
        ctx.channel().pipeline().fireUserEventTriggered(NettyTransport.SERVER_CLOSE_ACK);
    }

    @Override
    public synchronized void onChannelInactive(Endpoint endpoint) {
        endpoint.serverUp = true;
        endpoint.channel = null;
    }

    private synchronized void throwIfClosed() {
        if(closed) {
            throw new RuntimeException("Client is closed");
        }
    }

    public synchronized void shutdownGracefully() {
        connectWriteAndFlushSync();
        closed = true;
        transport.shutdown(()->{
            periodicFlusher.cancel(false);
            executor.shutdown();
        });
        Metrics.getInstance().shutdown();
    }

    /**
     * this method is not synchronized on purpose.
     * Making it synchronized would give opportunity for a deadlock as follows:
     * the periodic flusher has kicked in,
     * waiting to acquire the intrinsic lock, but this thread is holding the intrinsic lock,
     * waiting for the periodic flusher to shutdown!
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);
        timeoutNanos = transport.awaitTermination(timeoutNanos, TimeUnit.NANOSECONDS);
        log.info("Client has been shutdown");
        return executor.awaitTermination(timeoutNanos, TimeUnit.NANOSECONDS);
    }
}
