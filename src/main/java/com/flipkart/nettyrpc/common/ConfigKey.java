package com.flipkart.nettyrpc.common;

public enum ConfigKey {
    SERVER_LINGER_MS("server.linger.ms"),
    SERVER_LOW_WRITE_WATERMARK_BYTES("server.low.write.watermark.byes"),
    SERVER_HIGH_WRITE_WATERMARK_BYTES("server.high.write.watermark.bytes"),
    SERVER_READ_TIMEOUT_SECS("server.read.timeout.secs"),
    SERVER_BATCH_SIZE_BYTES("server.batch.size.bytes"),

    CLIENT_LINGER_MS("client.linger.ms"),
    CLIENT_BATCH_SIZE_BYTES("client.batch.size.bytes"),
    CLIENT_EXPIRY_INTERVAL_SECS("client.expiry.interval.secs"),
    CLIENT_READ_TIMEOUT_SECS("client.read.timeout.secs"),
    CLIENT_METRIC_DUMP_SECS("client.metric.dump.secs")
    ;
    public final String key;
    ConfigKey(String key) {
        this.key = key;
    }
}
