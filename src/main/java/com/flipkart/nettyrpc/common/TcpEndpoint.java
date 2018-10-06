package com.flipkart.nettyrpc.common;

public class TcpEndpoint extends Endpoint{
    public final int port;
    public final String host;
    private final String description;

    public TcpEndpoint(String host, int port) {
        this(host, port, 5_000_000_000L);
    }

    public TcpEndpoint(String host, int port, long reconnectWaitNanos) {
        super(reconnectWaitNanos);
        this.port = port;
        this.host = host;
        description = String.format("TcpEndpoint %s:%s", host, port);
    }

    @Override
    public String toString() {
        return description;
    }
}
