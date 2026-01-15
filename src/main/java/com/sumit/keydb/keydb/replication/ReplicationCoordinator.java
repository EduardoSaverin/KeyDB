package com.sumit.keydb.keydb.replication;

import com.sumit.keydb.keydb.common.ReplicationProperties;
import com.sumit.keydb.keydb.model.PendingWrite;
import com.sumit.keydb.keydb.model.Replica;
import com.sumit.keydb.keydb.model.ValueWithMeta;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Component
@Log4j2
public class ReplicationCoordinator {
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ExecutorService readingExecutor = Executors.newCachedThreadPool();
    private final ExecutorService repairExecutor = Executors.newCachedThreadPool();
    private final HintedHandoffService hintedHandoffService;
    private final List<Replica> replicas;
    private final AtomicLong versionClock = new AtomicLong();
    private final int N;
    private final int W;
    private final int R;

    public ReplicationCoordinator(List<Replica> replicas,
                                  ReplicationProperties replicationProperties,
                                  final HintedHandoffService hintedHandoffService) {
        this.replicas = replicas;
        this.hintedHandoffService = hintedHandoffService;
        this.N = replicationProperties.getN();
        this.W = replicationProperties.getW();
        this.R = replicationProperties.getR();
    }

    /* ================= WRITE ================= */
    public void put(String key, String value, long version) {
        CompletionService<Boolean> cs =
                new ExecutorCompletionService<>(executor);
        for (Replica replica : this.replicas) {
            cs.submit(() -> {
                try {
                    replica.engine().put(key, value, version);
                    return true;
                } catch (Exception e) {
                    log.error("Error in saving value with Replica: {}, Key: {}. Error: {}", replica.id(), key, e.getMessage());
                    this.hintedHandoffService.addHint(replica, new PendingWrite(key, value, version));
                }
                return false;
            });
        }
        awaitWriteQuorum(cs);
    }

    /* ================= READ ================= */
    public String get(String key) {
        CompletionService<ValueWithMeta> completionService = new ExecutorCompletionService<>(readingExecutor);
        for (Replica replica : this.replicas) {
            completionService.submit(() -> {
                try {
                    return replica.engine().getWithMeta(key);
                } catch (IOException e) {
                    return null;
                }
            });
        }
        List<ValueWithMeta> responses = new ArrayList<>();
        for (int i = 0; i < R; i++) {
            try {
                Future<ValueWithMeta> future = completionService.take();
                if (future.isDone() && future.get() != null) {
                    responses.add(future.get());
                }
            } catch (Exception ignored) {

            }
        }
        if (responses.isEmpty()) {
            return null;
        }
        ValueWithMeta latest = responses.stream().max(Comparator.comparing(ValueWithMeta::timestamp)).get();
        repair(key, latest);
        return latest.value();
    }

    private void repair(String key, ValueWithMeta latest) {
        log.debug("Repairing key: {} with latest value: {}", key, latest.value());
        for (Replica replica : this.replicas) {
            readingExecutor.submit(() -> {
                try {
                    ValueWithMeta local = replica.engine().getWithMeta(key);
                    if (local == null || local.timestamp() < latest.timestamp()) {
                        replica.engine().put(key, latest.value(), latest.timestamp());
                    }
                } catch (Exception ignored) {}
            });
        }
    }

    public void delete(String key) {
        long timestamp = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(this.W);

        for (Replica replica : replicas) {
            executor.submit(() -> {
                try {
                    replica.engine().delete(key);
                    latch.countDown();
                } catch (Exception e) {
                    this.hintedHandoffService.addHint(
                            replica,
                            new PendingWrite(key, null, timestamp)
                    );
                }
            });
        }

        awaitWriteQuorum(latch);
    }

    public void batchPut(Map<String, String> items) {
        long timestamp = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(this.W);

        for (Replica replica : replicas) {
            executor.submit(() -> {
                try {
                    replica.engine().batchPut(items);
                    latch.countDown();
                } catch (Exception e) {
                    for (var entry : items.entrySet()) {
                        this.hintedHandoffService.addHint(
                                replica,
                                new PendingWrite(entry.getKey(), entry.getValue(), timestamp)
                        );
                    }
                }
            });
        }

        awaitWriteQuorum(latch);
    }

    public Map<String, String> range(String start, String end) {
        CompletionService<Map<String, ValueWithMeta>> cs =
                new ExecutorCompletionService<>(executor);

        for (Replica replica : replicas) {
            cs.submit(() -> replica.engine().rangeWithMeta(start, end));
        }

        Map<String, ValueWithMeta> merged = new HashMap<>();

        for (int i = 0; i < this.R; i++) {
            try {
                Map<String, ValueWithMeta> result = cs.take().get();
                for (var e : result.entrySet()) {
                    merged.merge(
                            e.getKey(),
                            e.getValue(),
                            (a, b) -> a.timestamp() >= b.timestamp() ? a : b
                    );
                }
            } catch (Exception ignored) {}
        }

        // Async read repair
        repairRange(merged);

        return merged.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().value()
                ));
    }

    private void repairRange(Map<String, ValueWithMeta> latest) {
        for (Replica replica : replicas) {
            executor.submit(() -> {
                try {
                    for (var e : latest.entrySet()) {
                        ValueWithMeta local = replica.engine().getWithMeta(e.getKey());
                        if (local == null || local.timestamp() < e.getValue().timestamp()) {
                            replica.engine().put(
                                    e.getKey(),
                                    e.getValue().value(),
                                    e.getValue().timestamp()
                            );
                        }
                    }
                } catch (Exception ignored) {}
            });
        }
    }

    public void compaction() {
        for (Replica replica : replicas) {
            executor.submit(() -> {
                try {
                    replica.engine().compaction();
                } catch (Exception e) {
                    log.warn("Compaction failed for replica {}", replica.id(), e);
                }
            });
        }
    }

    private void awaitWriteQuorum(CountDownLatch latch) {
        try {
            if (!latch.await(this.N, TimeUnit.SECONDS)) {
                throw new RuntimeException("Write quorum not satisfied");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void awaitWriteQuorum(CompletionService<Boolean> cs) {
        int success = 0;
        while (success < W) {
            try {
                Future<Boolean> f = cs.take();
                if (Boolean.TRUE.equals(f.get())) {
                    success++;
                }
            } catch (Exception ignored) {}
        }
    }

    public long getVersionClock() {
        return versionClock.incrementAndGet();
    }

    public void awaitPendingWrites() {
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {}
    }

    public void compactionAndWait() {
        // 1. Stop accepting new writes
        awaitPendingWrites();

        // 2. Run compaction synchronously
        for (Replica replica : replicas) {
            try {
                replica.engine().compaction();
            } catch (Exception e) {
                log.warn("Compaction failed for replica {}", replica.id(), e);
            }
        }
    }
}
