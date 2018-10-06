package com.flipkart.nettyrpc.client;

import com.flipkart.nettyrpc.common.MessageMetadata;
import com.flipkart.nettyrpc.common.MessageParser;
import com.flipkart.nettyrpc.common.Utils;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

class Batch {
    private static final Logger log = LoggerFactory.getLogger(Batch.class);

    private final ByteBuf byteBuf;
    final Map<Long, MessageMetadata> messages = new HashMap<>();
    private final int batchSize;

    public Batch(ByteBuf byteBuf, int batchSize) {
        this.byteBuf = byteBuf;
        this.batchSize = batchSize;
    }


    boolean addMessage(MessageMetadata messageMetadata, byte[] data) {
        messages.put(messageMetadata.id, messageMetadata);
        MessageParser.writeMessage(byteBuf, messageMetadata, data);
        return byteBuf.readableBytes() > batchSize;
    }

    void failBatch(Throwable cause) {
        for (Map.Entry<Long, MessageMetadata> entry : messages.entrySet()) {
            MessageMetadata msg = entry.getValue();
            Utils.safelyFailCallback(msg.callback, cause, msg.attachment);
        }
    }

    public ByteBuf getByteBuf() {
        return byteBuf;
    }
}
