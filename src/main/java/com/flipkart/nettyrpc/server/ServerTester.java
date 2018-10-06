package com.flipkart.nettyrpc.server;

import com.flipkart.nettyrpc.common.TcpEndpoint;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Scanner;

public class ServerTester {
    private static final Logger log = LoggerFactory.getLogger(ServerTester.class);

    public static void main(String[] args) throws InterruptedException {
        NettyServer server = new NettyServer((request, ctx) -> {
            ByteBuf response = ctx.alloc(100);
            response.writeCharSequence("server says: ", Charset.defaultCharset());
            ctx.sendResponse(response);
        }, new TcpEndpoint("127.0.0.1", 8007));
        Scanner keyboard = new Scanner(System.in);
        log.info("type an integer to shutdown...");
        int myint = keyboard.nextInt();
        log.info("shutting down now....");

        server.shutdownGracefully();
    }
}


