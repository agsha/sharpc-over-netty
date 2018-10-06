package com.flipkart.nettyrpc.client;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.nettyrpc.common.GenericErrorHandler;
import com.flipkart.nettyrpc.common.MessageMetadata;
import com.flipkart.nettyrpc.common.Metrics;
import com.flipkart.nettyrpc.common.Utils;
import com.flipkart.nettyrpc.common.exceptions.ChannelClosedException;
import com.flipkart.nettyrpc.common.exceptions.TimeoutException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.flipkart.nettyrpc.common.Utils.desc;

class ChannelState {
    private final TimeoutException timeoutException;
    private final MetricRegistry registry;
    private final HashMap<Long, MessageMetadata> pendingResponse = new HashMap<>();
    private static final Logger log = LoggerFactory.getLogger(ChannelState.class);
    private boolean closing;
    private final long expiryTimeNanos;
    private final ChannelClosedException closedException;
    private long currentIterationTime = System.nanoTime();
    private final Histogram totalTimeNanos;
    private final Histogram dequeTimeNanos;
    private final Histogram sendCompleteNanos;
    private final Histogram responseTimeNanos;
    private final Histogram callbackTimeNanos;
    private final Meter responsePerSecond;
    private final List<Metrics.NameAndHistogram> histogramList;
    private String pendingResponseMetricName;
    private int count = 0;

    public ChannelState(long expiryTimeNanos, MetricRegistry registry) {
        this.expiryTimeNanos = expiryTimeNanos;
        timeoutException = new TimeoutException(String.format("no response received from server after %s seconds", expiryTimeNanos/1000_000_000));
        this.registry = registry;
        responsePerSecond = registry.meter("responsePerSecond");
        closedException = new ChannelClosedException("Channel has been closed");
        Metrics metrics = Metrics.getInstance();
        totalTimeNanos = new Histogram(3600000000000L, 3);
        dequeTimeNanos = new Histogram(3600000000000L, 3);
        sendCompleteNanos = new Histogram(3600000000000L, 3);
        responseTimeNanos = new Histogram(3600000000000L, 3);
        callbackTimeNanos = new Histogram(3600000000000L, 3);
        histogramList = new ArrayList<>();
        histogramList.add(new Metrics.NameAndHistogram("totalTimeNanos", totalTimeNanos));
        histogramList.add(new Metrics.NameAndHistogram("dequeTimeNanos", dequeTimeNanos));
        histogramList.add(new Metrics.NameAndHistogram("sendCompleteNanos", sendCompleteNanos));
        histogramList.add(new Metrics.NameAndHistogram("responseTimeNanos", responseTimeNanos));
        histogramList.add(new Metrics.NameAndHistogram("callbackTimeNanos", callbackTimeNanos));
        histogramList.add(new Metrics.NameAndHistogram("totalTimeNanos", totalTimeNanos));
    }

    public void addGaugeForPendingResponses(String metricName) {
        pendingResponseMetricName = "pendingResponses."+metricName;
        registry.register(pendingResponseMetricName, (Gauge<Long>) () -> (long) pendingResponse.size());

    }

    public void removeGaugeForPendingResponses() {
        registry.remove(pendingResponseMetricName);
    }

    public void dumpMetrics() {
        Metrics metrics = Metrics.getInstance();
        metrics.addValuesFrom(histogramList);
    }

    public void writeSuccess(Map<Long, MessageMetadata> messages) {
        pendingResponse.putAll(messages);
    }

    public void onMessage(ChannelHandlerContext ctx, long requestId, ByteBuf response) {
        try {
            MessageMetadata messageMetadata = pendingResponse.remove(requestId);
            if (messageMetadata == null) {
                log.warn("no messageMetadata found for requestId: {}", requestId);
                return;
            }
            messageMetadata.responseNanos = currentIterationTime;
            Utils.safelyCompleteCallback(messageMetadata.callback, response, messageMetadata.attachment);
            updateCurrentIterationTime();
            messageMetadata.callbackNanos = currentIterationTime;
            recordMetrics(messageMetadata);
        } finally {
            maybeClose(ctx);
        }
    }

    private void recordMetrics(MessageMetadata messageMetadata) {
        totalTimeNanos.recordValue(messageMetadata.callbackNanos - messageMetadata.createNanos);
        dequeTimeNanos.recordValue(messageMetadata.deqNanos - messageMetadata.createNanos);
        sendCompleteNanos.recordValue(messageMetadata.sendCompleteNanos - messageMetadata.deqNanos);
        responseTimeNanos.recordValue(messageMetadata.responseNanos - messageMetadata.sendCompleteNanos);
        callbackTimeNanos.recordValue(messageMetadata.callbackNanos - messageMetadata.responseNanos);
        final int ITERATIONS = 10;
        if(count == ITERATIONS) {
            responsePerSecond.mark(ITERATIONS);
            count = 0;
        }
        count++;
    }

    private void maybeClose(ChannelHandlerContext ctx) {
        if(closing) {
            if(pendingResponse.size() % 100 == 0) {
                log.info("trying to close channel {}, pending requests are {}", desc(ctx), pendingResponse.size());
            }

            if(pendingResponse.size() == 0) {
                log.info("closing the channel {} because pending requests are zero", desc(ctx));
                ctx.channel().close().addListener(GenericErrorHandler.instance);
            }
        }
    }

    public void setClosing(ChannelHandlerContext ctx, boolean closing) {
        this.closing = closing;
        maybeClose(ctx);
    }

    /**
     * goes through all pendingResponse messages and fails the messages that have expired
     */
    public void cleanup(ChannelHandlerContext ctx) {
        long now = System.nanoTime();
        Iterator<Map.Entry<Long, MessageMetadata>> it = pendingResponse.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, MessageMetadata> entry = it.next();
            MessageMetadata m = entry.getValue();
            if (now - m.createNanos > expiryTimeNanos) {
                Utils.safelyFailCallback(m.callback, timeoutException, m.attachment);
                it.remove();
            }
        }
        maybeClose(ctx);
    }

    public void failAll() {
        Iterator<Map.Entry<Long, MessageMetadata>> it = pendingResponse.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, MessageMetadata> entry = it.next();
            MessageMetadata m = entry.getValue();
            Utils.safelyFailCallback(m.callback, closedException, m.attachment);
            it.remove();
        }
    }

    public void updateCurrentIterationTime() {
        currentIterationTime = System.nanoTime();
    }
}
