package com.flipkart.nettyrpc.common;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Endpoint {
    private static final Logger log = LoggerFactory.getLogger(Endpoint.class);

    public final long reconnectWaitNanos; // 5 secs between consecutive reconnect attempts
    public Channel channel;
    public long lastReconnectAttemptNanos = 0;
    public boolean serverUp = true;
    public Exception connectFailureCause;

    public Endpoint(long reconnectWaitNanos) {
        this.reconnectWaitNanos = reconnectWaitNanos;
    }
}
