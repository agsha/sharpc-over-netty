package com.flipkart.nettyrpc.common;

public class UdsEndpoint extends Endpoint {
    public final String udsFile;
    private final String description;

    public UdsEndpoint(String udsFile) {
        this(udsFile, 5_000_000_000L);
    }

    public UdsEndpoint(String udsFile, long reconnectWaitNanos) {
        super(reconnectWaitNanos);
        this.udsFile = udsFile;
        description = String.format("UdsEndpoint %s", udsFile);
    }

    @Override
    public String toString() {
        return description;
    }
}
