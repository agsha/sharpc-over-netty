package com.flipkart.nettyrpc.client;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.nettyrpc.common.Endpoint;
import com.flipkart.nettyrpc.common.TcpEndpoint;
import com.flipkart.nettyrpc.utils.Utils;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientTesteer {
    private static final Logger log = LoggerFactory.getLogger(ClientTesteer.class);

    public static void main(String[] args) throws Exception {
        ClientTesteer obj = new ClientTesteer();
        obj.go();
    }

    private void go() throws Exception {
        MetricRegistry registry = new MetricRegistry();
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(3, TimeUnit.SECONDS);
        Endpoint endpoint = new TcpEndpoint("127.0.0.1", 8007);
        NettyClient client = new NettyClient(endpoint, new ArrayList<Endpoint>());
//        ServerSocketChannel ssc = ServerSocketChannel.open();
//        ssc.bind(new InetSocketAddress("127.0.0.1", 8007));
        client.connectSync(endpoint);
//        SocketChannel sc = ssc.accept();
//        sc.close();
        byte[] b = new byte[40];
        Utils.Timer t = new Utils.Timer("");
        AtomicBoolean boo = new AtomicBoolean(false);
        while(true) {
            client.send(b, new CompletionHandler<ByteBuf, Void>() {
                long now = System.nanoTime();
                @Override
                public void completed(ByteBuf result, Void attachment) {
//                    log.info("server returned: {}", result.readCharSequence(result.readableBytes(), Charset.defaultCharset()));
                    t.count();
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    log.error("failed!", exc);
                    System.exit(0);

                }
            }, null);
//            client.connectWriteAndFlushSync();
//            Thread.sleep(100);
        }
    }

}
