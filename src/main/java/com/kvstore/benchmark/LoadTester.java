package com.kvstore.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class LoadTester {

    private static final Logger log = LoggerFactory.getLogger(LoadTester.class);
    private static final String BASE_URL = "http://localhost:8080/api";

    private final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .executor(Executors.newFixedThreadPool(50))
        .build();

    public static void main(String[] args) throws Exception {
        LoadTester tester = new LoadTester();

        System.out.println("\n╔══════════════════════════════════════╗");
        System.out.println("║     KV Store Load Test Results       ║");
        System.out.println("╚══════════════════════════════════════╝\n");

        tester.warmUp();

        BenchmarkResult writeResult = tester.runWriteBenchmark(1000, 20);
        tester.printResult("WRITE (PUT)", writeResult);

        BenchmarkResult readResult = tester.runReadBenchmark(1000, 20);
        tester.printResult("READ  (GET)", readResult);

        BenchmarkResult mixedResult = tester.runMixedBenchmark(1000, 20);
        tester.printResult("MIXED 70/30", mixedResult);

        tester.printSummary(writeResult, readResult, mixedResult);
    }

    private void warmUp() throws Exception {
        log.info("Warming up...");
        for (int i = 0; i < 20; i++) {
            put("warmup-" + i, "value-" + i);
        }
        Thread.sleep(500);
        log.info("Warm up complete");
    }

    public BenchmarkResult runWriteBenchmark(int totalOps, int threads) throws Exception {
        log.info("Running WRITE benchmark: {} ops, {} threads", totalOps, threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failed  = new AtomicInteger(0);
        CountDownLatch latch  = new CountDownLatch(totalOps);

        long start = System.currentTimeMillis();

        for (int i = 0; i < totalOps; i++) {
            final int idx = i;
            pool.submit(() -> {
                long t0 = System.nanoTime();
                try {
                    int status = put("key-" + idx, "value-" + idx);
                    if (status == 200) success.incrementAndGet();
                    else failed.incrementAndGet();
                } catch (Exception e) {
                    failed.incrementAndGet();
                } finally {
                    latencies.add((System.nanoTime() - t0) / 1_000_000);
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;
        pool.shutdown();

        return buildResult(totalOps, threads, elapsed, latencies, success.get(), failed.get());
    }

    public BenchmarkResult runReadBenchmark(int totalOps, int threads) throws Exception {
        log.info("Running READ benchmark: {} ops, {} threads", totalOps, threads);
        for (int i = 0; i < 100; i++) put("read-key-" + i, "value-" + i);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failed  = new AtomicInteger(0);
        CountDownLatch latch  = new CountDownLatch(totalOps);
        Random random = new Random();

        long start = System.currentTimeMillis();

        for (int i = 0; i < totalOps; i++) {
            final int idx = random.nextInt(100);
            pool.submit(() -> {
                long t0 = System.nanoTime();
                try {
                    int status = get("read-key-" + idx);
                    if (status == 200) success.incrementAndGet();
                    else failed.incrementAndGet();
                } catch (Exception e) {
                    failed.incrementAndGet();
                } finally {
                    latencies.add((System.nanoTime() - t0) / 1_000_000);
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;
        pool.shutdown();

        return buildResult(totalOps, threads, elapsed, latencies, success.get(), failed.get());
    }

    public BenchmarkResult runMixedBenchmark(int totalOps, int threads) throws Exception {
        log.info("Running MIXED benchmark: {} ops, {} threads", totalOps, threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failed  = new AtomicInteger(0);
        CountDownLatch latch  = new CountDownLatch(totalOps);
        Random random = new Random();

        long start = System.currentTimeMillis();

        for (int i = 0; i < totalOps; i++) {
            final int idx = i;
            final boolean isRead = random.nextInt(10) < 7; // 70% reads
            pool.submit(() -> {
                long t0 = System.nanoTime();
                try {
                    int status = isRead
                        ? get("key-" + (idx % 100))
                        : put("key-" + idx, "value-" + idx);
                    if (status == 200) success.incrementAndGet();
                    else failed.incrementAndGet();
                } catch (Exception e) {
                    failed.incrementAndGet();
                } finally {
                    latencies.add((System.nanoTime() - t0) / 1_000_000);
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;
        pool.shutdown();

        return buildResult(totalOps, threads, elapsed, latencies, success.get(), failed.get());
    }

    private BenchmarkResult buildResult(int totalOps, int threads, long elapsedMs,
            List<Long> latencies, int success, int failed) {
        Collections.sort(latencies);
        double throughput = (success * 1000.0) / elapsedMs;
        long p50 = percentile(latencies, 50);
        long p95 = percentile(latencies, 95);
        long p99 = percentile(latencies, 99);
        long avg = (long) latencies.stream().mapToLong(l -> l).average().orElse(0);
        return new BenchmarkResult(totalOps, threads, elapsedMs,
            throughput, avg, p50, p95, p99, success, failed);
    }

    private long percentile(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, idx));
    }

    private int put(String key, String value) throws Exception {
        String body = "{\"value\":\"" + value + "\"}";
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/keys/" + key))
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(body))
            .timeout(Duration.ofSeconds(5))
            .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    private int get(String key) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(BASE_URL + "/keys/" + key))
            .GET()
            .timeout(Duration.ofSeconds(5))
            .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    private void printResult(String name, BenchmarkResult r) {
        System.out.printf("┌─────────────────────────────────────┐%n");
        System.out.printf("│ %-35s │%n", name);
        System.out.printf("├─────────────────────────────────────┤%n");
        System.out.printf("│ Throughput:  %7.0f req/s           │%n", r.throughput);
        System.out.printf("│ Avg latency: %7d ms              │%n", r.avgMs);
        System.out.printf("│ p50 latency: %7d ms              │%n", r.p50Ms);
        System.out.printf("│ p95 latency: %7d ms              │%n", r.p95Ms);
        System.out.printf("│ p99 latency: %7d ms              │%n", r.p99Ms);
        System.out.printf("│ Success:     %7d / %-7d       │%n", r.success, r.totalOps);
        System.out.printf("│ Duration:    %7d ms              │%n", r.elapsedMs);
        System.out.printf("└─────────────────────────────────────┘%n%n");
    }

    private void printSummary(BenchmarkResult w, BenchmarkResult r, BenchmarkResult m) {
        System.out.println("╔══════════════════════════════════════╗");
        System.out.println("║           RESUME NUMBERS             ║");
        System.out.println("╠══════════════════════════════════════╣");
        System.out.printf( "║  Write throughput: %6.0f req/s       ║%n", w.throughput);
        System.out.printf( "║  Read  throughput: %6.0f req/s       ║%n", r.throughput);
        System.out.printf( "║  Mixed throughput: %6.0f req/s       ║%n", m.throughput);
        System.out.printf( "║  Write p99 latency: %5d ms          ║%n", w.p99Ms);
        System.out.printf( "║  Read  p99 latency: %5d ms          ║%n", r.p99Ms);
        System.out.println("╚══════════════════════════════════════╝");
    }

    public record BenchmarkResult(
        int totalOps, int threads, long elapsedMs,
        double throughput, long avgMs,
        long p50Ms, long p95Ms, long p99Ms,
        int success, int failed
    ) {}
}
