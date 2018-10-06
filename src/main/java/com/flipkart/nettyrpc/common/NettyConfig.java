package com.flipkart.nettyrpc.common;

import java.util.Map;

public class NettyConfig {
    private Map<String, String> configMap;

    public NettyConfig(Map<String, String> configMap) {
        this.configMap = configMap;
    }

    public int getInt(ConfigKey key) {
        if(configMap.containsKey(key.key)) {
            return Integer.parseInt(configMap.get(key.key));
        }
        switch (key) {
            case SERVER_LINGER_MS:
                return 10;
            case SERVER_LOW_WRITE_WATERMARK_BYTES:
                return 5*1024;
            case SERVER_HIGH_WRITE_WATERMARK_BYTES:
                return 15*1024;
            case SERVER_READ_TIMEOUT_SECS:
                return 100;
            case SERVER_BATCH_SIZE_BYTES:
                return 10*1024;
            case CLIENT_LINGER_MS:
                return 10;
            case CLIENT_BATCH_SIZE_BYTES:
                return 10*1024;
            case CLIENT_EXPIRY_INTERVAL_SECS:
                return 100;
            case CLIENT_READ_TIMEOUT_SECS:
                return 100;
            case CLIENT_METRIC_DUMP_SECS:
                return 2;
            default:
                throw new IllegalArgumentException("unknown key");

        }
    }
}
