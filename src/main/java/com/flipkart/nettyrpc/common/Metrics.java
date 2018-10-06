package com.flipkart.nettyrpc.common;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Metrics {

    private static final Metrics metrics = new Metrics();
    private final Map<String, Histogram> histogramMap = new HashMap<>();
    private MetricRegistry metricRegistry;
    private int[] percentiles;
    private volatile boolean initialized = false;
    private java.util.concurrent.ScheduledFuture<?> histogramCleaner;

    public static Metrics getInstance() {
        return metrics;
    }
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    // private constructor for singleton
    private Metrics() {
    }

    public synchronized void init(MetricRegistry metricRegistry, int[] percentiles, ScheduledExecutorService executor) {
        if(initialized) {
            log.warn("metrics has already been initialized");
            return;
        }
        this.metricRegistry = metricRegistry;
        this.percentiles = percentiles;
        histogramCleaner = executor.scheduleAtFixedRate(this::resetAll, 5, 5, TimeUnit.SECONDS);
        initialized = true;
    }

    public synchronized MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public synchronized void addValuesFrom(List<NameAndHistogram> histogramList) {
        for (NameAndHistogram nameAndHistogram : histogramList) {
            Histogram existing = getHistogram(nameAndHistogram.name);
            nameAndHistogram.histogram.copyInto(existing);
            nameAndHistogram.histogram.reset();
        }
    }

    /**
     * Let percentiles be {50, 90, 99} (for example)
     * For every recorder, the code will create three gauges:
     * name.50.percentile, name.90.percentile, name.99.percentile
     *
     * @param name of the recorder
     * @return the recorder
     */
    public synchronized Histogram getHistogram(String name) {
        if(percentiles == null) {
            throw new RuntimeException("Metrics has not been initialized. Please call init() first.");
        }
        if (histogramMap.containsKey(name)) {
            return histogramMap.get(name);
        }
        Histogram histogram = new Histogram(3600000000000L, 3);
        histogramMap.put(name, histogram);
        for (int percentile : percentiles) {
            String metricName = String.format("%s.%d.percentile", name, percentile);
            metricRegistry.register(metricName, (Gauge<Long>) () -> histogram.getValueAtPercentile(percentile));
        }
        return histogram;
    }

    private synchronized void resetAll() {
        for (Histogram histogram : histogramMap.values()) {
            histogram.reset();
        }
    }

    public static class NameAndHistogram {
        final String name;
        public final Histogram histogram;

        public NameAndHistogram(String name, Histogram histogram) {
            this.name = name;
            this.histogram = histogram;
        }
    }

    // for await termination, the caller has to shutdown the executor
    // (that has been passed to this instance)
    // and awaitTermination on that executor
    public synchronized void shutdown() {
        histogramCleaner.cancel(false);
    }
}
