package com.sumit.keydb.keydb;

import com.sumit.keydb.keydb.common.ReplicationProperties;
import com.sumit.keydb.keydb.engine.KeyDBEngine;
import com.sumit.keydb.keydb.model.Replica;
import com.sumit.keydb.keydb.replication.HintedHandoffService;
import com.sumit.keydb.keydb.replication.ReplicationCoordinator;
import com.sumit.keydb.keydb.utils.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ReplicationCoordinatorTest {

    @TempDir
    Path tempDir;
    private ReplicationCoordinator replicationCoordinator;
    private List<KeyDBEngine> engines;

    @BeforeEach
    void setup() throws Exception {
        engines = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Path replicaPath = tempDir.resolve("replica-" + i);
            Files.createDirectories(replicaPath);
            engines.add(TestUtils.createEngine(replicaPath));
        }

        List<Replica> replicas = List.of(
                new Replica("r1", engines.get(0)),
                new Replica("r2", engines.get(1)),
                new Replica("r3", engines.get(2))
        );

        ReplicationProperties props = new ReplicationProperties();
        props.setN(3);
        props.setW(3);
        props.setR(3);
        replicationCoordinator = new ReplicationCoordinator(replicas, props, new HintedHandoffService());
    }

    /* ================= BASIC PUT / GET ================= */
    @Test
    void testPutAndGet() {
        replicationCoordinator.put("key", "value", this.replicationCoordinator.getVersionClock());
        String value = replicationCoordinator.get("key");
        assertEquals("value", value);
    }

    /* ================= DELETE ================= */
    @Test
    void testDelete() {
        replicationCoordinator.put("key", "value", this.replicationCoordinator.getVersionClock());
        replicationCoordinator.delete("key");
        assertNull(replicationCoordinator.get("key"));

        // Eventual Consistency Check
        engines.forEach(e ->
                assertNull(assertDoesNotThrow(() -> e.get("key")))
        );
    }

    /* ================= BATCH PUT ================= */
    @Test
    void testBatchPut() {
        Map<String, String> map = Map.of(
                "k1", "v1",
                "k2", "v2"
        );
        this.replicationCoordinator.batchPut(map);
        assertEquals("v1", replicationCoordinator.get("k1"));
        assertEquals("v2", replicationCoordinator.get("k2"));
    }

    /* ================= RANGE ================= */
    @Test
    void testRange() {
        Map<String, String> map = Map.of(
                "a", "1","b", "2", "c", "3", "d", "4"
        );
        this.replicationCoordinator.batchPut(map);
        Map<String, String> result = this.replicationCoordinator.range("b", "c");
        assertEquals(2, result.size());
        assertEquals("2", result.get("b"));
        assertEquals("3", result.get("c"));
    }

    /* ================= CONCURRENT WRITES ================= */
    @Test
    void testConcurrentWrites() throws InterruptedException {
        int threads = 8;
        int opsPerThread = 200;

        try (ExecutorService executorService = Executors.newFixedThreadPool(threads)) {
            CountDownLatch latch = new CountDownLatch(threads);

            for (int i = 0; i < threads; i++) {
                int threadId = i;
                long version = this.replicationCoordinator.getVersionClock();
                executorService.submit(() -> {
                    try {
                        for (int j = 0; j < opsPerThread; j++) {
                            replicationCoordinator.put(
                                    "key-" + threadId + "-" + j,
                                    "value-" + j,
                                    version
                            );
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(60, TimeUnit.SECONDS));
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
        }
        replicationCoordinator.awaitPendingWrites();

        for (int t = 0; t < threads; t++) {
            for (int j = 0; j < opsPerThread; j++) {
                assertEquals(
                        "value-" + j,
                        replicationCoordinator.get("key-" + t + "-" + j)
                );
            }
        }
    }

    /* ================= COMPACTION ================= */
    @Test
    void testCompaction() throws IOException {
        for (int i = 0; i < 200; i++) {
            this.replicationCoordinator.put("key" + i, "value" + i, this.replicationCoordinator.getVersionClock());
        }

        this.replicationCoordinator.compactionAndWait();
        sleep(1000);
        for (int i = 0; i < 200; i++) {
            assertEquals(this.replicationCoordinator.get("key" + i), "value" + i);
        }
    }

    /* ================= READ REPAIR ================= */
    @Test
    void testReadRepair() throws IOException {
        long timestamp = this.replicationCoordinator.getVersionClock();
        replicationCoordinator.put("key", "value", this.replicationCoordinator.getVersionClock());

        // Manually corrupt one replica
        engines.getFirst().put("key", "old", timestamp - 10_000);

        assertEquals("value", replicationCoordinator.get("key"));
        sleep(1000);
        for (KeyDBEngine engine : engines) {
            assertEquals("value", engine.get("key"));
        }
    }

    /* ================= UTIL ================= */
    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {}
    }
}
