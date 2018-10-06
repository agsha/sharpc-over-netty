package com.flipkart.nettyrpc.server;

import com.flipkart.nettyrpc.common.MessageParser;
import com.flipkart.nettyrpc.common.TcpEndpoint;
import com.flipkart.nettyrpc.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import static org.junit.jupiter.api.Assertions.fail;

public class NettyServerTest_DataSanity {
    private static final Logger log = LoggerFactory.getLogger(NettyServerTest_DataSanity.class);

    @Test
    public void testFoo() throws IOException, InterruptedException {
        testEndToEnd(20, 10000, 10, 2000);
    }

    /**
     * spawns a number of mock clients (numClients). Each client sends numMessages of checksum prepended random content and random length
     * The server callback verifies the checksum and sends a response of checksummed random content and random length.
     * The response of the server is checksum verified on the client
     */
    private void testEndToEnd(int numClients, int numMessages, int minSize, int maxSize) throws InterruptedException, IOException {
        int port = 19838;
        NettyServer server = new NettyServer((request, ctx) -> {
            TestUtils.verifyChecksum(request);
            ByteBuf response = Unpooled.wrappedBuffer(TestUtils.randomArrayWithCrc(minSize, maxSize));
            ctx.sendResponse(response);
        }, new TcpEndpoint("127.0.0.1", port));


        List<Connection> connections = new ArrayList<>();

        for (int i = 0; i < numClients; i++) {
            SocketChannel sc = SocketChannel.open();
            connections.add(new Connection(sc));
            sc.connect(new InetSocketAddress("127.0.0.1", port));
        }

        for(int i=0; i<numMessages; i++) {
            for (Connection conn : connections) {
                if(conn.requests < numMessages) {
                    writeRandomMessage(conn, minSize, maxSize);
                }
                readMessage(conn);
            }
        }


        while (true) {
            if(connections.size() == 0) {
                break;
            }
            Iterator<Connection> iterator = connections.iterator();
            while (iterator.hasNext()) {
                Connection conn = iterator.next();
                readMessage(conn);
                if(conn.responses == numMessages) {
                    log.debug("closing a socket. numsockets is {}", connections.size());
                    conn.sc.close();
                    iterator.remove();
                }
            }
        }
        server.shutdownGracefully();
    }

    private void writeRandomMessage(Connection conn, int minSize, int maxSize) throws IOException {
        SocketChannel sc = conn.sc;
        ByteBuffer byteBuffer = TestUtils.randomMessageWithCrc(conn.requests++, minSize, maxSize).nioBuffer();
        while (byteBuffer.remaining() > 0) {
            sc.write(byteBuffer);
        }
    }

    private void readMessage(Connection conn) throws IOException {
        SocketChannel sc = conn.sc;
        ByteBuf readBuffer = conn.bf;

        TestUtils.readIntoByteBuf(sc, readBuffer, conn.nioBuf);
        conn.parser.onData(readBuffer);
        readBuffer.discardSomeReadBytes();
    }

    static class Connection implements MessageParser.Callback {
        final SocketChannel sc;
        final ByteBuf bf = Unpooled.buffer(10_000);
        final ByteBuffer nioBuf = ByteBuffer.allocate(10_000);
        final MessageParser parser = new MessageParser(this, new UnpooledByteBufAllocator(false));

        long requests = 0;
        long responses = 0;
        Connection(SocketChannel sc) {
            this.sc = sc;
        }

        @Override
        public void onMessage(long requestId, ByteBuf msg) {
            TestUtils.verifyChecksum(msg);
            responses++;
        }

        @Override
        public void onControlMessage(MessageParser.Command command) {
            fail("was not expecting a control message");
        }

        @Override
        public void onParseError(MessageParser.ParseError error) {
            fail(String.format("%s %s", error.description, error.description));
        }
    }
}
