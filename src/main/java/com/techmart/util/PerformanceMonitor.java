package com.techmart.util;

import com.techmart.model.PerformanceMetric;

import jakarta.annotation.PostConstruct;
import jakarta.ejb.Lock;
import jakarta.ejb.LockType;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Application-scoped performance monitor.
 *
 * Collects timing samples from EJBs and JMS components and exposes
 * aggregated statistics to the metrics dashboard.
 *
 * Using @Singleton with @Startup so the monitor is always ready before
 * any other component tries to record a metric.  Read operations use
 * LockType.READ to allow concurrent reads; writes use LockType.WRITE
 * only where ConcurrentHashMap is insufficient.
 */
@Singleton
@Startup
public class PerformanceMonitor {

    private static final Logger LOG = Logger.getLogger(PerformanceMonitor.class.getName());

    // Keep a rolling window of the last 500 metrics to avoid unbounded growth
    private static final int MAX_HISTORY = 500;

    // Per-component invocation counters
    private final Map<String, AtomicLong> invocationCounts = new ConcurrentHashMap<>();

    // Per-component total elapsed time (ms) — for avg latency calculation
    private final Map<String, AtomicLong> totalElapsedMs = new ConcurrentHashMap<>();

    // Rolling list of individual metric snapshots
    private final List<PerformanceMetric> metricHistory =
            Collections.synchronizedList(new ArrayList<>());

    // Application start time
    private LocalDateTime startTime;

    @PostConstruct
    public void init() {
        this.startTime = LocalDateTime.now();
        LOG.info("PerformanceMonitor initialized at " + startTime);
    }

    /**
     * Records a timing sample for the given component/operation pair.
     *
     * @param component  e.g. "ProductCatalogBean", "OrderProcessorMDB"
     * @param operation  e.g. "searchProducts", "processOrder"
     * @param elapsedMs  wall-clock elapsed time in milliseconds
     */
    public void record(String component, String operation, long elapsedMs) {
        String key = component + "." + operation;

        invocationCounts.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
        totalElapsedMs.computeIfAbsent(key, k -> new AtomicLong(0)).addAndGet(elapsedMs);

        PerformanceMetric metric = new PerformanceMetric(component, operation, elapsedMs, "ms");

        synchronized (metricHistory) {
            if (metricHistory.size() >= MAX_HISTORY) {
                metricHistory.remove(0);
            }
            metricHistory.add(metric);
        }
    }

    /**
     * Returns average latency (ms) for a given component.operation key.
     */
    @Lock(LockType.READ)
    public double getAverageLatency(String component, String operation) {
        String key = component + "." + operation;
        AtomicLong count = invocationCounts.get(key);
        AtomicLong total = totalElapsedMs.get(key);
        if (count == null || count.get() == 0) return 0.0;
        return (double) total.get() / count.get();
    }

    /**
     * Returns total invocation count for a given component.operation key.
     */
    @Lock(LockType.READ)
    public long getInvocationCount(String component, String operation) {
        String key = component + "." + operation;
        AtomicLong count = invocationCounts.get(key);
        return count == null ? 0 : count.get();
    }

    /**
     * Snapshot of all tracked keys with their invocation counts and avg latency.
     * Used by the metrics dashboard.
     */
    @Lock(LockType.READ)
    public List<Map<String, Object>> getSummary() {
        List<Map<String, Object>> summary = new ArrayList<>();
        for (String key : invocationCounts.keySet()) {
            AtomicLong count = invocationCounts.get(key);
            AtomicLong total = totalElapsedMs.get(key);
            double avg = (count != null && count.get() > 0)
                    ? (double) total.get() / count.get() : 0.0;

            Map<String, Object> row = new ConcurrentHashMap<>();
            row.put("key", key);
            row.put("invocations", count != null ? count.get() : 0L);
            row.put("avgLatencyMs", avg);
            summary.add(row);
        }
        return summary;
    }

    /**
     * Returns a copy of the recent metric history (last MAX_HISTORY entries).
     */
    @Lock(LockType.READ)
    public List<PerformanceMetric> getRecentMetrics() {
        synchronized (metricHistory) {
            return new ArrayList<>(metricHistory);
        }
    }

    @Lock(LockType.READ)
    public LocalDateTime getStartTime() {
        return startTime;
    }
}
