package com.flipkart.nettyrpc.client;

import com.flipkart.nettyrpc.common.MessageMetadata;
import com.flipkart.nettyrpc.common.MessageParser;
import com.flipkart.nettyrpc.common.TcpEndpoint;
import com.flipkart.nettyrpc.common.exceptions.ServerShuttingDownException;
import com.flipkart.nettyrpc.utils.TestUtils;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class NettyClientTest_DataSanity {
    private static final Logger log = LoggerFactory.getLogger(MessageParser.class);

    private static final int port = 19838;
    @Test
    /**
     * Test that data corruption does not happen.
     */
    void testDataSanity() throws IOException, InterruptedException, ServerShuttingDownException {
        testDataSanity(100_000, 10_000-100, 10_000+100);
    }

    /**
     * Uses NettyClient to send numMessages random messages.
     * Each message is a random message of a random length between minPayloadSize and maxPayloadSize.
     * The first 8 bytes of each message is a checksum of the rest of the message
     *
     * The mock server is a simple blocking tcp server that understands the protocol and validates the checksum.
     */
    private void testDataSanity(int numMessages, int minPayloadSize, int maxPayloadSize) throws IOException, InterruptedException {
        DefaultCompletionHandler handler = new DefaultCompletionHandler(numMessages);
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress("127.0.0.1", port));

        // start the mock server
        new Thread(() -> {
            try {
                mockServer(ssc);
            } catch (IOException e) {
                fail(e);
            }
        }).start();

        // start the client
        NettyClient client = new NettyClient(new TcpEndpoint("127.0.0.1", port), Lists.newArrayList());
        for(int i=0; i<numMessages; i++) {
            byte[] data = TestUtils.randomArrayWithCrc(minPayloadSize, maxPayloadSize);
            client.send(data, handler, null);
        }
        client.shutdownGracefully();
        client.awaitTermination(10, TimeUnit.SECONDS);
        handler.latch.await();
    }


    private void mockServer(ServerSocketChannel ssc) throws IOException {
        final CRC32 crc32 = new CRC32();
        SocketChannel sc = ssc.accept();
        ByteBuf bf = Unpooled.buffer();

        byte[] data = {};
        MessageParser.Callback callback = new MessageParser.Callback() {
            @Override
            public void onMessage(long requestId, ByteBuf msg) {
                long crc = msg.readLong();
                crc32.reset();
                crc32.update(msg.nioBuffer());
                assertEquals( crc, crc32.getValue());

                ByteBuf response = Unpooled.buffer();
                MessageParser.writeMessage(response, new MessageMetadata(requestId, System.nanoTime(), null, null), data);
                try {
                    ByteBuffer nioBuffer = response.nioBuffer();
                    while (nioBuffer.remaining() > 0) {
                        sc.write(nioBuffer);
                    }
                } catch (IOException e) {
                    fail();
                }
                bf.discardSomeReadBytes();
            }

            @Override
            public void onControlMessage(MessageParser.Command command) {
                fail("this shouldn't happen");
            }

            @Override
            public void onParseError(MessageParser.ParseError error) {
                fail(String.format("parse error %s %s", error.description, error.object.toString()));
            }
        };


        ByteBuffer nioBuf = ByteBuffer.allocate(10_000);
        MessageParser parser = new MessageParser(callback, new UnpooledByteBufAllocator(false));
        while(true) {
            boolean closed = TestUtils.readIntoByteBuf(sc, bf, nioBuf);
            parser.onData(bf);
            if(closed) {
                break;
            }
        }
        sc.close();
        ssc.close();
    }

    static class DefaultCompletionHandler implements CompletionHandler<ByteBuf, Void> {

        private int iterations;
        final CountDownLatch latch = new CountDownLatch(1);

        DefaultCompletionHandler(int iterations) {
            this.iterations = iterations;
        }

        @Override
        public void completed(ByteBuf result, Void attachment) {
            iterations--;
            if(iterations==0) {
                latch.countDown();
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            throw new RuntimeException("failed", exc);
        }
    }

}
