package com.flipkart.nettyrpc.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.flipkart.nettyrpc.common.MessageParser.State.*;

/**
 * Format of messages:
 * Every message has a "protocol" part which looks like this
 *
 *   |  MAGIC (long)  |  format (short)  |
 *
 * The rest of the message depends on the value of the format field.
 *
 * Currently two formats are supported
 *
 * F1 (Format 1 -- regular messages)
 *   |  MAGIC (long)  |  format=1 (short)  |   requestId (long)  | timestamp (long)  | message_body_length (int) | message_body (message_body_length bytes) |
 *
 *
 * F2 (Format 2 -- control messages)
 *
 *   |  MAGIC (long)  |  format=2 (short)  |  timestamp (long) |  command (short) |
 *
 * currently supported commands are CLOSE (command=1)
 *
 */
public class MessageParser {
    private static final Logger log = LoggerFactory.getLogger(MessageParser.class);

    private final Callback callback;
    private final ByteBufAllocator allocator;
    private static final long MAGIC = 0x5ca1ab1e;
    // the current state of the state machine
    private State state = State.PROTOCOL;
    private final Parser[] parsers = new Parser[]{new PreambleParser(), new F1Parser(), new F2Parser()};
    private final Command[] commands = Command.values();

    public MessageParser(Callback callback, ByteBufAllocator allocator) {
        this.callback = callback;
        this.allocator = allocator;
    }


    public void onData(ByteBuf msg) {
        while (parsers[state.parserIndex].parse(msg)) {
        }
    }


    /**
     * understands the MAGIC and the format fields and transfers control to the
     * actual formatter
     */
    class PreambleParser implements Parser {
        private static final int PROTOCOL_SIZE = 10;

        @Override
        public boolean parse(ByteBuf msg) {
            if (state != PROTOCOL) {
                callback.onParseError(new ParseError("Unrecognized state", state));
                return false;
            }
            if (msg.readableBytes() < PROTOCOL_SIZE) {
                return false;
            }
            long magic = msg.readLong();
            if (magic != MAGIC) {
                callback.onParseError(new ParseError(String.format("incorrect magic header found. expected %d, got %d", MAGIC, magic), new Object()));
                return false;
            }
            short format = msg.readShort();
            if (format < 1 || format > parsers.length) {
                callback.onParseError(new ParseError(String.format("unrecognized format %d", format), new Object()));
                return false;
            }
            Parser parser = parsers[format];
            state = parser.getInitialState();
            return true;
        }

        @Override
        public State getInitialState() {
            return PROTOCOL;
        }
    }

    /**
     * Parser for format 1 (F1) messages
     */
    class F1Parser implements Parser {
        private long requestId = 0, timestamp = 0;
        private int len = -1;
        private final int HEADER_SIZE = 20;

        public boolean parse(ByteBuf msg) {
            switch (state) {
                case F1HEADER:
                    if (msg.readableBytes() < HEADER_SIZE) {
                        return false;
                    }
                    requestId = msg.readLong();
                    timestamp = msg.readLong();
                    len = msg.readInt();
                    state = State.F1BODY;
                    // fall through
                case F1BODY:
                    if (msg.readableBytes() < len) {
                        return false;
                    }
                    ByteBuf data = allocator.buffer(len);
                    msg.readBytes(data, len);
                    callback.onMessage(requestId, data);
                    ReferenceCountUtil.release(data);
                    requestId = 0;
                    timestamp = 0;
                    len = -1;
                    state = State.PROTOCOL;
            }
            return true;
        }

        @Override
        public State getInitialState() {
            return F1HEADER;
        }
    }

    class F2Parser implements Parser {
        private int command = -1;
        private long timestamp = -1;

        private final int HEADER_SIZE = 12;

        public boolean parse(ByteBuf msg) {
            switch (state) {
                case F2COMMAND:
                    if (msg.readableBytes() < HEADER_SIZE) {
                        return false;
                    }
                    command = msg.readInt();
                    timestamp = msg.readLong();
                    callback.onControlMessage(commands[command]);
                    command = -1;
                    timestamp = -1;
                    state = State.PROTOCOL;
            }
            return true;
        }

        @Override
        public State getInitialState() {
            return F2COMMAND;
        }
    }

    public enum Command {CLOSE}

    enum State {
        PROTOCOL(0), F1HEADER(1), F1BODY(1), F2COMMAND(2);

        // index into the @parsers array.
        // this field indicates the index of the parser
        // which knows how to handle this state
        final int parserIndex;

        State(int parserIndex) {
            this.parserIndex = parserIndex;
        }
    }

    interface Parser {
        /**
         * Returns true if there was sufficient bytes to transition the state
         * false if there was insufficient bytes.
         */
        boolean parse(ByteBuf msg);

        State getInitialState();
    }

    public interface Callback {
        void onMessage(long requestId, ByteBuf msg);

        void onControlMessage(Command command);

        void onParseError(ParseError error);
    }

    public static void writeMessage(ByteBuf byteBuf, MessageMetadata messageMetadata, byte[] data) {
        byteBuf.writeLong(MessageParser.MAGIC).writeShort(1).writeLong(messageMetadata.id).writeLong(messageMetadata.createNanos).writeInt(data.length).writeBytes(data);
    }
    public static void writeMessage(ByteBuf byteBuf, long requestId, ByteBuf message) {
        byteBuf.writeLong(MessageParser.MAGIC).writeShort(1).writeLong(requestId).writeLong(System.nanoTime()).writeInt(message.readableBytes()).writeBytes(message);
    }

    public static ByteBuf getControlMessage(Command command) {
        return Unpooled.buffer().writeLong(MessageParser.MAGIC).writeShort(2).writeInt(command.ordinal()).writeLong(System.nanoTime());
    }

    public static class ParseError {
        public final String description;
        public final Object object;

        ParseError(String description, Object object) {
            this.description = description;
            this.object = object;
        }
    }
}
