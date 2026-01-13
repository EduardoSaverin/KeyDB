package com.sumit.keydb.keydb;

import com.sumit.keydb.keydb.common.StorageConfig;
import com.sumit.keydb.keydb.engine.KeyDBEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class KeyDBEngineTest {

    private KeyDBEngine keyDBEngine;
    private Path tempDir;

    @BeforeEach
    void setup() throws IOException {
        this.tempDir = Files.createTempDirectory("keydb-tests");

        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setMaxFileSize(256);
        storageConfig.setDataDir(this.tempDir.toString());

        this.keyDBEngine = new KeyDBEngine(storageConfig);
        this.keyDBEngine.initialize();
    }

    @AfterEach
    void teardown() throws IOException {
        this.keyDBEngine.close();
        try(Stream<Path> stream = Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())){
            stream.forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
            });
        }
    }

    /* ===================== Basic Operations ===================== */
    @Test
    void testPutAndGet() throws IOException {
        this.keyDBEngine.put("key", "value");
        this.keyDBEngine.put("alpha", "beta");

        assertEquals("value", this.keyDBEngine.get("key"));
        assertEquals("beta", this.keyDBEngine.get("alpha"));
        assertNull(this.keyDBEngine.get("missing"));
    }

    @Test
    void testOverwriteKey() throws IOException {
        this.keyDBEngine.put("key", "value");
        this.keyDBEngine.put("key", "value2");

        assertEquals("value2", this.keyDBEngine.get("key"));
    }

    @Test
    void testBatch() throws IOException {
        Map<String, String> map = Map.of(
                "k1", "v1",
                "k2", "v2",
                "k3", "v3"
        );
        this.keyDBEngine.batchPut(map);
        assertEquals("v1", this.keyDBEngine.get("k1"));
        assertEquals("v2", this.keyDBEngine.get("k2"));
        assertEquals("v3", this.keyDBEngine.get("k3"));
    }

    @Test
    void testRangeQuery() throws IOException {
        this.keyDBEngine.put("a", "1");
        this.keyDBEngine.put("b", "2");
        this.keyDBEngine.put("c", "3");
        this.keyDBEngine.put("d", "4");

        Map<String, String> result = this.keyDBEngine.range("b", "c");

        assertEquals(2, result.size());
        assertEquals("2", result.get("b"));
        assertEquals("3", result.get("c"));
    }

    /* ===================== Persistence ===================== */
    @Test
    void testRestartRecovery() throws IOException {
        this.keyDBEngine.put("key", "value");
        this.keyDBEngine.put("alpha", "beta");

        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setMaxFileSize(256);
        storageConfig.setDataDir(this.tempDir.toString());
        this.keyDBEngine = new KeyDBEngine(storageConfig);
        this.keyDBEngine.initialize();

        assertEquals("value", this.keyDBEngine.get("key"));
        assertEquals("beta", this.keyDBEngine.get("alpha"));
    }

    /* ===================== Compaction ===================== */
    @Test
    void testCompaction() throws IOException {
        for (int i = 0; i < 100; i++) {
            this.keyDBEngine.put("k" + i, "v" + i);
        }

        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setMaxFileSize(256);
        storageConfig.setDataDir(this.tempDir.toString());
        this.keyDBEngine = new KeyDBEngine(storageConfig);
        this.keyDBEngine.initialize();
        this.keyDBEngine.compaction();

        for (int i = 0; i < 100; i++) {
            assertEquals("v" + i, this.keyDBEngine.get("k" + i));
        }
    }

    /* ===================== Concurrency ===================== */
    @Test
    void testConcurrentReadAndWrite() throws IOException, InterruptedException {
        int threads = 8;
        int opsPerThread = 500;
        try (ExecutorService executorService = Executors.newFixedThreadPool(threads)) {
            CountDownLatch latch = new CountDownLatch(threads);

            for (int i = 0; i < threads; i++) {
                int threadId = i;
                executorService.submit(() -> {
                    try {
                        for (int j = 0; j < opsPerThread; j++) {
                            this.keyDBEngine.put("k-" + threadId + "-" + j, "v-" + j);
                        }
                    } catch (IOException e) {
                        fail(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            assertTrue(latch.await(60, TimeUnit.SECONDS));
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
        }

        // Verify
        for (int t = 0; t < threads; t++) {
            for (int j = 0; j < opsPerThread; j++) {
                assertEquals("v-" + j, this.keyDBEngine.get("k-" + t + "-" + j));
            }
        }
    }

    /* ===================== Throughput / Speed ===================== */
    @Test
    void testWriteThroughput() throws IOException {
        int totalWrites = 50_000;
        long startTime = System.nanoTime();
        for (int i = 0; i < totalWrites; i++) {
            this.keyDBEngine.put("k" + i, "v" + i);
        }
        long endTime = System.nanoTime();

        double seconds = (endTime - startTime) / 1_000_000_000.0;
        double opsPerSecond = totalWrites / seconds;

        System.out.println("Write throughput: " + opsPerSecond);
        assertTrue(opsPerSecond > 5_000);
    }

    @Test
    void testReadThroughput() throws IOException {
        int total = 20_000;

        for (int i = 0; i < total; i++) {
            this.keyDBEngine.put("k" + i, "v" + i);
        }

        long startTime = System.nanoTime();
        for (int i = 0; i < total; i++) {
            assertEquals("v" + i, this.keyDBEngine.get("k" + i));
        }
        long endTime = System.nanoTime();
        double seconds = (endTime - startTime) / 1_000_000_000.0;
        double opsPerSecond = total/seconds;
        System.out.println("Read throughput: " + opsPerSecond);
        assertTrue(opsPerSecond > 10_000);
    }
}
