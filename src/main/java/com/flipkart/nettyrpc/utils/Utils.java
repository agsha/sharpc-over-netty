package com.flipkart.nettyrpc.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by sharath.g on 09/06/15.
 */
public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static class LatencyTimer extends Thread {
        private static char noname = 'A';
        private final String name;
        private LatPrinter printer;
        public AtomicLong[] bins = new AtomicLong[4000];
        public AtomicLong maxNanos;
        public AtomicLong lastCount = new AtomicLong(0);

        double[] pTiles = new double[]{1, 50, 75, 90, 95, 99, 99.9};


        public LatencyTimer(String name) {
            this(new LatDefaultPrinter(), name);
        }


        public LatencyTimer(Class name) {
            this(new LatDefaultPrinter(), name.getName());
        }

        public LatencyTimer() {
            this(new LatDefaultPrinter(), "" + noname);
            noname += 1;
        }

        public LatencyTimer(LatPrinter p) {
            this(p, "noname");
        }

        public LatencyTimer(LatPrinter p, String name) {
            this.name = name;
            this.printer = p;
            reset();
            setDaemon(true);
            start();
        }

        public void setPrinter(LatPrinter printer) {
            this.printer = printer;
        }

        public void count(long latencyNanos) {
            int index = 0;
            maxNanos.set(Math.max(maxNanos.get(), latencyNanos));
            while (latencyNanos >= 1000) {
                latencyNanos /= 1000;
                index += 1000;
            }


            bins[(int) Math.min(index + latencyNanos, bins.length - 1)].incrementAndGet();
        }

        public void count() {
            long now = System.nanoTime();
            count(Math.max(now - lastCount.get(), 0));
            lastCount.set(now);
        }

        public void reset() {
            for (int i = 0; i < bins.length; i++) {
                bins[i] = new AtomicLong(0);
            }
            maxNanos = new AtomicLong(0);
        }

        AtomicBoolean die = new AtomicBoolean(false);

        public void die() {
            die.set(true);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (die.get()) {
                    return;
                }
                printer.log(name, snap());
            }
        }


        public interface LatPrinter {
            void log(String name, LatRet ret);
        }


        public void doLog() {
            printer.log(name, snap());
        }

        private LatRet snap() {
            long[] mybins = new long[bins.length];
            long mytotal = 0;
            for (int i = 0; i < bins.length; i++) {
                mybins[i] = bins[i].get();
                mytotal += mybins[i];
            }

            long myMaxNanos = maxNanos.get();

            double[] nanos = new double[pTiles.length];
            int index = 0;
            long cumulative = 0;
            for (int i = 0; i < pTiles.length; i++) {
                long max = (long) ((mytotal * pTiles[i]) / 100.0);
                while (index < mybins.length && mybins[index] + cumulative < max) {
                    cumulative += mybins[index];
                    index++;
                }

                long mul = 1;
                int temp = index;
                while (temp >= 1000) {
                    temp -= 1000;
                    mul *= 1000;
                }
                nanos[i] = (temp + 1) * mul;
            }
            reset();
            return new LatRet(mytotal, myMaxNanos, nanos, pTiles);
        }

        public static class LatDefaultPrinter implements LatPrinter {
            @Override
            public void log(String name, LatRet ret)
            {
                log.info("{}, {}", name, ret);
            }
        }


        public static class LatRet implements Serializable {
            public double[] nanos;
            public double[] pTiles;
            public long total;
            public long maxNanos;
            public long snapTimeMillis;

            @Override
            public String toString() {
                if (total == 0) {
                    return "No data points";
                }
                DecimalFormat df = new DecimalFormat("###.##");
                String s = String.format("max:%s", timeFormat(maxNanos, df));
                for (int i = nanos.length - 1; i >= 0; i--) {
                    s += pTiles[i] + "%:" + timeFormat(nanos[i], df);
                }
                return s;
            }

            public static String timeFormat(double t, DecimalFormat df) {
                if (t < 1000) {
                    return df.format(t) + "ns ";
                } else if (t < 1000_000) {
                    return df.format(t / 1000) + "us ";
                } else if (t < 1000_000_000) {
                    return df.format(t / 1000_000) + "ms ";
                } else {
                    return df.format(t / 1000_000_000) + "s ";
                }
            }

            //for objectmapper
            public LatRet() {
            }

            public LatRet(long total, long maxNanos, double[] nanos, double[] pTiles) {
                this.nanos = nanos;
                this.pTiles = pTiles;
                this.total = total;
                this.maxNanos = maxNanos;
                this.snapTimeMillis = System.currentTimeMillis();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        //log.debug("{}", httpPostJson("http://posttestserver.com/post.php", "f u"));
        //log.debug("{}", httpPostJson("http://httpbin.org/post", "f u"));
        Utils u = new Utils();
        u.go();
    }

    private void go() throws InterruptedException {
        Timer t = new Timer("ASfd");
        int c = 0;
        for (int i = 0; i < 10000000; i++) {
            t.cumulativeCount(c++);
            Thread.sleep(100);
        }

    }

    public static class Timer extends Thread {

        private String name;
        long beginTime = -1, lastSnapshotTime = -1;
        AtomicLong opsSoFar = new AtomicLong(0);
        AtomicLong opsSinceLastSnapshot = new AtomicLong(0);
        private Printer printer;
        public AtomicBoolean enabled = new AtomicBoolean(true);

        public void reset() {
            lastSnapshotTime = beginTime = System.nanoTime();
            opsSinceLastSnapshot.set(0);
            opsSoFar.set(0);
//            log.debug("======resetting timer====");
        }

        public Timer(String name) {
            this(new DefaultPrinter(), name);
        }


        public Timer(Class name) {
            this(new DefaultPrinter(), name.getName());
        }

        public Timer(Printer p) {
            this(p, "noname");
        }

        public Timer(Printer p, String name) {
            this.name = name;
            this.printer = p;
            setDaemon(true);
            reset();
            start();
        }

        public void setPrinter(Printer printer) {
            this.printer = printer;
        }

        AtomicBoolean die = new AtomicBoolean(false);

        public void die() {
            die.set(true);
        }

        @Override
        public void run() {
            reset();

            while (true) {
                try {
                    sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (enabled.get()) doLog();
                if (interrupted()) break;
                if (die.get()) {
                    break;
                }
            }
        }


        public void doLog() {
            printer.log(name, snap());
        }

        public void count(long opsSinceLastCall) {
            opsSinceLastSnapshot.addAndGet(opsSinceLastCall);
        }

        public void count() {
            opsSinceLastSnapshot.incrementAndGet();
        }

        public void cumulativeCount(long cumulative) {
            opsSinceLastSnapshot.set(cumulative - opsSoFar.get());
        }

        public Ret snap() {
            if (beginTime < 0) throw new RuntimeException("not initialized");
            long now = System.nanoTime();
            long ops = opsSinceLastSnapshot.getAndSet(0);
            long cumulativeOps = opsSoFar.addAndGet(ops);

            double qps = ops * 1e9 / (now - lastSnapshotTime);
            double totalqps = (cumulativeOps) * 1e9 / (now - beginTime);

            lastSnapshotTime = now;

            return new Ret((long) qps, (long) totalqps, ops, cumulativeOps);
        }

        public static class Ret implements Serializable {
            public long qps, totalQps, ops, totalOps;

            public Ret(long qps, long totalQps, long ops, long totalOps) {
                this.qps = qps;
                this.totalQps = totalQps;
                this.ops = ops;
                this.totalOps = totalOps;
            }

            // for objectmapper
            public Ret() {
            }

            @Override
            public String toString() {
                return String.format("qps:%s ops:%s", format(qps), format(totalOps));
            }

            String format(long x) {
                String s = "" + x;
                StringBuilder ss = new StringBuilder();
                for (int i = 0; i < s.length(); i++) {
                    if (i % 3 == 0 && i > 0) {
                        ss.append("_");
                    }
                    ss.append(s.charAt(s.length() - i - 1));

                }
//                return ss.reverse().toString();
                return ss.reverse().toString();
            }
        }

        public interface Printer {
            void log(String name, Ret ret);
        }

        public static class DefaultPrinter implements Printer {
            @Override
            public void log(String name, Ret ret) {
                log.info("{} {}", name, ret);
            }
        }
    }
}
