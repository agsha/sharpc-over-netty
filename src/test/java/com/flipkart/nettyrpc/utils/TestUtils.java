package com.flipkart.nettyrpc.utils;

import com.flipkart.nettyrpc.common.MessageMetadata;
import com.flipkart.nettyrpc.common.MessageParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.CRC32;
import static org.junit.jupiter.api.Assertions.*;

public class TestUtils {
    // returns a random int between min inclusive and max exclusive
    private static int random(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max);
    }

    private static void fillRandom(byte[] data) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        random.nextBytes(data);
    }

    public static byte[] randomArrayWithCrc(int minSize, int maxSize) {
        CRC32 crc32 = new CRC32();
        int len = random(minSize, maxSize);
        byte[] data = new byte[len];
        fillRandom(data);
        crc32.update(data, 8, data.length - 8);
        long checksum = crc32.getValue();
        ByteBuffer.wrap(data).putLong(0, checksum);
        return data;
    }

    public static void verifyChecksum(ByteBuf bf) {
        CRC32 crc32 = new CRC32();
        long actualChecksum = bf.readLong();
        crc32.update(bf.nioBuffer());
        long expectedChecksum = crc32.getValue();
        assertEquals(actualChecksum, expectedChecksum);
    }

    public static ByteBuf randomMessageWithCrc(long requestId, int minSize, int maxSize) {
        byte[] data = randomArrayWithCrc(minSize, maxSize);
        ByteBuf buffer = Unpooled.buffer(data.length + 100);
        MessageParser.writeMessage(buffer, new MessageMetadata(requestId, System.nanoTime(), null, null), data);
        return buffer;
    }

    public static boolean readIntoByteBuf(SocketChannel sc, ByteBuf bf, ByteBuffer nioBf) throws IOException {
        nioBf.clear();
        sc.configureBlocking(false);
        boolean closed = false;
        while(true) {
            int read = sc.read(nioBf);
            if(read < 0) {
                closed = true;
            }
            if(nioBf.remaining() == 0 || read <= 0) {
                break;
            }
        }
        int read = nioBf.flip().remaining();
        nioBf.clear();
        ByteBuf temp = Unpooled.wrappedBuffer(nioBf);
        temp.writerIndex(read);
        bf.writeBytes(temp);
        sc.configureBlocking(true);
        return closed;
    }
}
