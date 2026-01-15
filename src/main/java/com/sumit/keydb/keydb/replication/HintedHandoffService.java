package com.sumit.keydb.keydb.replication;

import com.sumit.keydb.keydb.model.PendingWrite;
import com.sumit.keydb.keydb.model.Replica;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

@Service
@Log4j2
public class HintedHandoffService {
    private final Map<Replica, Queue<PendingWrite>> hints = new ConcurrentHashMap<>();

    public HintedHandoffService() {
        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            scheduler.scheduleWithFixedDelay(this::retry, 5, 5, TimeUnit.SECONDS);
        }
    }

    public void addHint(Replica replica, PendingWrite pendingWrite) {
        hints.computeIfAbsent(replica, k -> new ConcurrentLinkedQueue<>()).add(pendingWrite);
    }

    public void retry() {
        log.debug("Running hinted handoff service");
        for (var entry : hints.entrySet()) {
            Queue<PendingWrite> queue = entry.getValue();
            PendingWrite write;
            while ((write = queue.poll()) != null) {
                try {
                    Replica r = entry.getKey();
                    r.engine().put(write.key(), write.value(), System.currentTimeMillis());
                } catch (Exception e) {
                    queue.add(write);
                    log.warn("Retry failed, will retry later");
                }
            }
        }
        log.debug("Finished hinted handoff service");
    }
}
