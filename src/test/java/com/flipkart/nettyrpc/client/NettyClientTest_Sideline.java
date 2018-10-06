package com.flipkart.nettyrpc.client;

import com.flipkart.nettyrpc.common.MessageMetadata;
import com.flipkart.nettyrpc.common.MessageParser;
import com.flipkart.nettyrpc.common.TcpEndpoint;
import com.flipkart.nettyrpc.common.exceptions.ServerShuttingDownException;
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

import static org.junit.jupiter.api.Assertions.fail;

public class NettyClientTest_Sideline {
    private static final int port = 19838;
    private static final Logger log = LoggerFactory.getLogger(NettyClientTest_Sideline.class);

    /**
     * Tests fallback behaviour of the client. If the primary is down, then the client connects and sends the message to the fallback (sideline)
     * The server is a mock tcp server that understands the protocol.
     */
    @Test
    public void testSideline() throws IOException, InterruptedException, ServerShuttingDownException {
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress("127.0.0.1", port+1));

        new Thread(() -> {
            try {
                mockServer(ssc);
            } catch (IOException e) {
                fail(e);
            }
        }).start();

        // there is no server listening on primary port.
        // the client should send to the secondary server
        NettyClient client = new NettyClient(new TcpEndpoint("127.0.0.1", port), Lists.newArrayList(new TcpEndpoint("127.0.0.1", port+1)));
        DefaultCompletionHandler callback = new DefaultCompletionHandler();
        client.send("hello".getBytes(), callback, null);
        client.shutdownGracefully();
        callback.latch.await();
    }

    private void mockServer(ServerSocketChannel ssc) throws IOException {
        SocketChannel sc = ssc.accept();
        ByteBuf bf = Unpooled.buffer();

        byte[] data = {};
        MessageParser.Callback callback = new MessageParser.Callback() {
            @Override
            public void onMessage(long requestId, ByteBuf msg) {
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
            nioBuf.clear();
            int read = sc.read(nioBuf);
            if(read == -1) {
                break;
            }
            nioBuf.clear();

            ByteBuf temp = Unpooled.wrappedBuffer(nioBuf);
            temp.writerIndex(read);

            bf.writeBytes(temp);
            parser.onData(bf);

        }
        sc.close();
        ssc.close();
    }

    static class DefaultCompletionHandler implements CompletionHandler<ByteBuf, Void> {

        final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void completed(ByteBuf result, Void attachment) {
            latch.countDown();
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            fail(exc);
            throw new RuntimeException("failed", exc);
        }
    }

}
